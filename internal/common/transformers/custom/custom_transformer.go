package custom

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync/atomic"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/transformers"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type CancelFunction func() error

const (
	InitialisationState int32 = iota
	ValidationState
	TransformationState
	ErrorState
)

type CustomTransformer struct {
	*transformers.TransformerBase
	executable   string
	validateArgs []string
	runArgs      []string
	cmd          *exec.Cmd
	inChan       chan []byte
	outChan      chan []byte
	errChan      chan error
	state        int32
	eg           *errgroup.Group
	gtx          context.Context
}

func NewCustomTransformer(
	base *transformers.TransformerBase,
	executable string, args ...string,
) *CustomTransformer {
	return &CustomTransformer{
		TransformerBase: base,
		executable:      executable,
		runArgs:         args,
	}
}

func (ct *CustomTransformer) init(ctx context.Context, args ...string) (CancelFunction, error) {
	// TODO:
	// 	1. You shouldn't wait for ct.cmd.Wait() instead you have to receive ValidationComplete message and keep
	//	   process running
	// 	2. Check the goroutine with defer outWriter.Close(). Ensure that closing pipes in tis way is required
	ctx, cancel := context.WithCancel(ctx)
	ct.state = InitialisationState
	ct.cmd = exec.CommandContext(ctx, ct.executable, args...)

	stdout, err := ct.cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("unable to open stdout pipe")
	}

	stderr, err := ct.cmd.StderrPipe()
	if err != nil {
		stdout.Close()
		cancel()
		return nil, fmt.Errorf("unable to open stdout pipe")
	}
	stdin, err := ct.cmd.StdinPipe()
	if err != nil {
		stdout.Close()
		stderr.Close()
		cancel()
		return nil, fmt.Errorf("unable to open stdout pipe")
	}

	ct.inChan = make(chan []byte, 1)

	ct.eg, ct.gtx = errgroup.WithContext(ctx)

	cancelFunction := func() error {
		cancel()
		if err := ct.eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn().Msgf("error in closing function: %s", err.Error())
		}
		stdout.Close()
		stderr.Close()
		stdin.Close()
		close(ct.inChan)
		return nil
	}

	if err := ct.cmd.Start(); err != nil {
		cancelFunction()
		return nil, fmt.Errorf("external command runtime error: %w", err)
	}

	ct.eg.Go(func() error {
		return ct.stderrReader(ct.gtx, stderr)
	})

	ct.eg.Go(func() error {
		return ct.stdoutReader(ct.gtx, stdout)
	})

	ct.eg.Go(func() error {
		return ct.stdinWriter(ct.gtx, stdin)
	})

	select {
	case <-ct.gtx.Done():
		return nil, ct.gtx.Err()
	default:
	}

	return cancelFunction, nil
}

func (ct *CustomTransformer) Wait() error {
	if err := ct.cmd.Wait(); err != nil {
		return fmt.Errorf("custom transformer runtime error: %w", err)
	}
	//log.Debug().Msg("custom transformer exited normally")
	return nil

}

func (ct *CustomTransformer) stdinWriter(ctx context.Context, stdin io.Writer) error {
	if ct.inChan == nil {
		return fmt.Errorf("channel is not initialized")
	}
	for {
		select {
		case data := <-ct.inChan:
			//log.Debug().Str("data", string(data)).Msg("received sending tuple from channel: forwarding to pipe")
			_, err := stdin.Write(data)
			if err != nil {
				return fmt.Errorf("send data to stdin: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (ct *CustomTransformer) stderrReader(ctx context.Context, stdout io.Reader) error {
	ct.errChan = make(chan error, 1)
	defer close(ct.errChan)
	lineScanner := bufio.NewReader(stdout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line, _, err := lineScanner.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		log.Warn().Str("output", string(line)).Msg("stderr forwarding from custom transformer")
	}
}

func (ct *CustomTransformer) stdoutReader(ctx context.Context, stderr io.Reader) error {
	ct.outChan = make(chan []byte, 1)
	defer close(ct.outChan)
	lineScanner := bufio.NewReader(stderr)
	for {
		line, _, err := lineScanner.ReadLine()
		line = append(line, '\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ct.outChan <- line:
			//log.Debug().Str("data", string(line)).Msg("received tuple from transformer: sent to channel")
		}
	}
}

func (ct *CustomTransformer) setErrState() {
	atomic.StoreInt32(&ct.state, ErrorState)
}

func (ct *CustomTransformer) getState() int32 {
	return atomic.LoadInt32(&ct.state)
}

func (ct *CustomTransformer) Init(ctx context.Context) (CancelFunction, error) {
	return ct.init(ctx, ct.runArgs...)
}

func (ct *CustomTransformer) Validate(ctx context.Context) domains.RuntimeErrors {
	// Must start process validate and exit
	var errs domains.RuntimeErrors
	cancel, err := ct.init(ctx, ct.validateArgs...)
	if err != nil {
		errs = append(errs, domains.NewRuntimeError().SetErr(err).SetMsg("transformer initialisation error"))
	}
	defer cancel()
	validationErrs := ct.validate(ctx)
	errs = append(errs, validationErrs...)
	if errs != nil {
		return errs
	}
	return nil
}

func (ct *CustomTransformer) validate(ctx context.Context) domains.RuntimeErrors {
	// TODO: Validation logic here
	return nil
}

func (ct *CustomTransformer) Transform(data []byte) ([]byte, error) {
	if err := ct.SendOriginalTuple(data); err != nil {
		return nil, fmt.Errorf("cannot send tuple to transformer: %w", err)
	}
	return ct.ReceiveTransformedTuple()
}

func (ct *CustomTransformer) SendOriginalTuple(data []byte) error {
	select {
	case ct.inChan <- data:
		//log.Debug().Str("data", string(data)).Msg("sent tuple to transformer: sent to channel")
	case <-ct.gtx.Done():
		return ct.gtx.Err()
	}

	return nil
}

func (ct *CustomTransformer) ReceiveTransformedTuple() ([]byte, error) {
	//log.Debug().Msg("trying to receive the message from transformer")
	for {
		// TODO: I don't know why but this code locks even when we send message to outChan
		//		 though it must receive the message and continue execution. That's why I added
		//		 loop there. But it mustn't here
		select {
		case <-ct.gtx.Done():
			return nil, ct.gtx.Err()
		case data := <-ct.outChan:
			//log.Debug().Str("data", string(data)).Msg("received tuple from transformer: from channel")
			if len(data) == 0 {
				return nil, fmt.Errorf("received empty tupple after trasnformation")
			}
			return data, nil
		default:
		}
	}

}
