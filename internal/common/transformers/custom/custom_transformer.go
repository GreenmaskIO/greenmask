package custom

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

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

const (
	ValidateArgName   = "--validate"
	ValidationTimeout = 20 * time.Second
)

type CustomTransformer struct {
	*transformers.TransformerBase
	executable string
	args       []string
	cmd        *exec.Cmd
	inChan     chan []byte
	outChan    chan []byte
	errChan    chan *domains.ValidationWarning
	eg         *errgroup.Group
	gtx        context.Context
}

func NewCustomTransformer(
	base *transformers.TransformerBase,
	executable string, args ...string,
) *CustomTransformer {
	return &CustomTransformer{
		TransformerBase: base,
		executable:      executable,
		args:            args,
	}
}

func (ct *CustomTransformer) init(ctx context.Context, args ...string) (CancelFunction, error) {
	// TODO:
	// 	1. You shouldn't wait for ct.cmd.Wait() instead you have to receive ValidationComplete message and keep
	//	   process running
	// 	2. Check the goroutine with defer outWriter.Close(). Ensure that closing pipes in tis way is required
	ctx, cancel := context.WithCancel(ctx)
	ct.cmd = exec.CommandContext(ctx, ct.executable, args...)

	stdoutReader, stdoutWriter := io.Pipe()
	ct.cmd.Stdout = stdoutWriter

	stderrReader, stderrWriter := io.Pipe()
	ct.cmd.Stderr = stderrWriter

	stdin, err := ct.cmd.StdinPipe()
	if err != nil {
		stdoutWriter.Close()
		stderrWriter.Close()
		stdoutReader.Close()
		stderrReader.Close()
		cancel()
		return nil, fmt.Errorf("unable to open stdout pipe")
	}

	ct.inChan = make(chan []byte, 1)

	ct.eg, ct.gtx = errgroup.WithContext(ctx)

	cancelFunction := func() error {
		cancel()
		stdin.Close()
		stdoutWriter.Close()
		stderrWriter.Close()
		if err := ct.eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn().Msgf("error in closing function: %s", err.Error())
		}
		stdoutReader.Close()
		stderrReader.Close()
		close(ct.inChan)
		return nil
	}

	if err := ct.cmd.Start(); err != nil {
		cancelFunction()
		return nil, fmt.Errorf("external command runtime error: %w", err)
	}

	ct.eg.Go(func() error {
		return ct.stderrReader(ct.gtx, stderrReader)
	})

	ct.eg.Go(func() error {
		return ct.stdoutReader(ct.gtx, stdoutReader)
	})

	ct.eg.Go(func() error {
		return ct.stdinWriter(ct.gtx, stdin)
	})

	return cancelFunction, nil
}

func (ct *CustomTransformer) Wait() error {
	if err := ct.cmd.Wait(); err != nil {
		return fmt.Errorf("custom transformer runtime error: %w", err)
	}
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
	ct.errChan = make(chan *domains.ValidationWarning, 10)
	defer close(ct.errChan)
	return lineReader(ctx, stdout, func(line []byte) error {
		var re domains.ValidationWarning
		if err := json.Unmarshal(line, &re); err != nil {
			log.Warn().Str("data", string(line)).Msgf("stderr forwarding")
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case ct.errChan <- &re:
		}
		return nil
	})
}

func (ct *CustomTransformer) stdoutReader(ctx context.Context, stdout io.Reader) error {
	ct.outChan = make(chan []byte, 1)
	defer close(ct.outChan)
	return lineReader(ctx, stdout, func(line []byte) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ct.outChan <- line:
			return nil
		}
	})
}

func (ct *CustomTransformer) Init(ctx context.Context) (CancelFunction, error) {
	return ct.init(ctx, ct.args...)
}

func (ct *CustomTransformer) Validate(ctx context.Context) (domains.ValidationWarnings, error) {
	// Must start process validate and exit
	args := make([]string, 0, len(ct.args)+2)
	args = append(args, ct.args...)
	//args = append(args, ValidateArgName)
	cancelFunction, err := ct.init(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("transformer initialisation error: %w", err)
	}
	defer cancelFunction()
	warnings, err := ct.validate(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot perform transformer validation: %w", err)
	}
	return warnings, nil
}

func (ct *CustomTransformer) validate(ctx context.Context) (domains.ValidationWarnings, error) {
	var res domains.ValidationWarnings
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	eg, gtx := errgroup.WithContext(ctx)

	doneChan := make(chan struct{})

	eg.Go(func() error {
		defer close(doneChan)
		if err := ct.cmd.Wait(); err != nil {
			return fmt.Errorf("custom transformer runtime error: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		for {
			select {
			case <-gtx.Done():
				return gtx.Err()
			case <-doneChan:
				return nil
			case re := <-ct.errChan:
				res = append(res, re)
			}
		}
	})

	eg.Go(func() error {
		for {
			select {
			case <-gtx.Done():
				return gtx.Err()
			case <-doneChan:
				return nil
			case data := <-ct.outChan:
				if len(data) > 0 {
					log.Warn().Str("data", string(data)).Msg("stdout forwarding from custom transformer")
				}
			}
		}
	})

	if err := eg.Wait(); err != nil && len(res) == 0 {
		return nil, err
	}
	return res, nil
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
	case <-ct.gtx.Done():
		return ct.gtx.Err()
	}

	return nil
}

func (ct *CustomTransformer) ReceiveTransformedTuple() ([]byte, error) {
	for {
		// TODO: I don't know why but this code locks even when we send message to outChan
		//		 though it must receive the message and continue execution. That's why I added
		//		 loop there. But it mustn't be here
		select {
		case <-ct.gtx.Done():
			return nil, ct.gtx.Err()
		case data := <-ct.outChan:
			if len(data) == 0 {
				return nil, fmt.Errorf("received empty tupple after trasnformation")
			}
			return data, nil
		default:
		}
	}
}

func lineReader(ctx context.Context, r io.Reader, lineHook func(line []byte) error) error {
	lineScanner := bufio.NewReader(r)
	defer func() {
		for {
			line, _, err := lineScanner.ReadLine()
			if err != nil {
				return
			}
			if err := lineHook(line); err != nil {
				return
			}
		}
	}()

	for {
		line, _, err := lineScanner.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
				return nil
			}
			return err
		}

		if err := lineHook(line); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}
