package greenmask_tmp

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"

	"github.com/greenmaskio/greenmask/internal/domains"
)

type CancelFunction func() error

const (
	InitialisationState int32 = iota
	ValidationState
	TransformationState
	ErrorState
)

const (
	ValidateArgName    = "--validate"
	PrintConfigArgName = "--print-config"
	MetaArgName        = "--meta"
	ValidationTimeout  = 20 * time.Second
)

type ReaderFunction func(ctx context.Context, r io.Reader) error
type WriterFunction func(ctx context.Context, r io.Writer) error

type CustomTransformer struct {
	*utils.TransformerBase
	executable   string
	args         []string
	cmd          *exec.Cmd
	inChan       chan []byte
	outChan      chan []byte
	errChan      chan *domains.ValidationWarning
	settingsChan chan *TransformerSettings
	eg           *errgroup.Group
	gtx          context.Context
	settings     *TransformerSettings
}

func NewCustomTransformer(
	ctx context.Context,
	base *utils.TransformerBase,
	executable string, args ...string,
) (*CustomTransformer, error) {
	ct := &CustomTransformer{
		TransformerBase: base,
		executable:      executable,
		args:            args,
	}
	overridenArgs := append(args[0:], PrintConfigArgName)

	cancelFunction, err := ct.init(ctx, overridenArgs, ct.lineStdinWriter, ct.getConfigStdoutReader, ct.transformationStderrReader)
	if err != nil {
		return nil, fmt.Errorf("cannot get transformer settings: %s", err)
	}
	defer cancelFunction()

	var settings *TransformerSettings
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case settings = <-ct.settingsChan:
	}
	ct.settings = settings

	return ct, nil
}

func (ct *CustomTransformer) InitTransformation(ctx context.Context) (CancelFunction, error) {
	// TODO: Generate table meta and pass it through the parameter encoded by base64
	meta, err := ct.getEncodedMetadata()
	overridenArgs := append(ct.args[0:], MetaArgName, meta)
	if err != nil {
		return nil, fmt.Errorf("cannot get metatda: %w", err)
	}
	return ct.init(ctx, overridenArgs, ct.lineStdinWriter, ct.transformationStdoutReader, ct.transformationStderrReader)
}

func (ct *CustomTransformer) Validate(ctx context.Context) (domains.ValidationWarnings, error) {
	// TODO: Depending on transformer setting we can either validate or not. Ensure this logic has been implemented
	meta, err := ct.getEncodedMetadata()
	overridenArgs := append(ct.args[0:], ValidateArgName, MetaArgName, meta)
	cancelFunction, err := ct.init(ctx, overridenArgs, ct.lineStdinWriter, ct.validationStdoutReader, ct.validationStderrReader)
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

func (ct *CustomTransformer) Transform(data []byte) ([]byte, error) {
	if err := ct.sendOriginalTuple(data); err != nil {
		return nil, fmt.Errorf("cannot send tuple to transformer: %w", err)
	}
	res, err := ct.receiveTransformedTuple()
	if err != nil {
		return nil, fmt.Errorf("cannot receive transformerd tuple from transformer: %w", err)
	}
	return res, nil
}

func (ct *CustomTransformer) getEncodedMetadata() (string, error) {
	var src []byte
	if err := json.Unmarshal(src, ct.TransformerBase.Table); err != nil {
		return "", fmt.Errorf("cannot unmarshal metadata: %w", err)
	}
	dst := make([]byte, 0, len(src))
	base64.StdEncoding.Encode(src, dst)
	return string(dst), nil
}

func (ct *CustomTransformer) init(ctx context.Context, args []string,
	stdinWriterFunc WriterFunction, stdoutReaderFunc ReaderFunction, stderrReaderFunc ReaderFunction,
) (CancelFunction, error) {
	if stderrReaderFunc == nil || stdoutReaderFunc == nil {
		return nil, errors.New("stderrReaderFunc and stdoutReaderFunc cannot be nil")
	}

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
		return stderrReaderFunc(ct.gtx, stderrReader)
	})

	ct.eg.Go(func() error {
		return stdoutReaderFunc(ct.gtx, stdoutReader)
	})

	ct.eg.Go(func() error {
		return stdinWriterFunc(ct.gtx, stdin)
	})

	return cancelFunction, nil
}

func (ct *CustomTransformer) lineStdinWriter(ctx context.Context, stdin io.Writer) error {
	if ct.inChan == nil {
		return fmt.Errorf("channel is not initialized")
	}
	for {
		select {
		case data := <-ct.inChan:
			_, err := stdin.Write(data)
			if err != nil {
				return fmt.Errorf("send data to stdin: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (ct *CustomTransformer) transformationStderrReader(ctx context.Context, stdout io.Reader) error {
	return lineReader(ctx, stdout, func(line []byte) error {
		log.Warn().Str("data", string(line)).Msgf("stderr forwarding")
		return nil
	})
}

func (ct *CustomTransformer) transformationStdoutReader(ctx context.Context, stdout io.Reader) error {
	ct.settingsChan = make(chan *TransformerSettings)
	defer close(ct.settingsChan)
	return lineReader(ctx, stdout, func(line []byte) error {
		var ts = &TransformerSettings{}
		if err := json.Unmarshal(line, ts); err != nil {
			log.Warn().Str("data", string(line)).Msgf("stdout forwarding")
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ct.settingsChan <- ts:
			return nil
		}
	})
}

func (ct *CustomTransformer) validationStderrReader(ctx context.Context, stdout io.Reader) error {
	return lineReader(ctx, stdout, func(line []byte) error {
		log.Warn().Str("data", string(line)).Msgf("stderr forwarding")
		return nil
	})
}

func (ct *CustomTransformer) validationStdoutReader(ctx context.Context, stdout io.Reader) error {
	ct.errChan = make(chan *domains.ValidationWarning, 10)
	defer close(ct.errChan)
	return lineReader(ctx, stdout, func(line []byte) error {
		var vw domains.ValidationWarning
		if err := json.Unmarshal(line, &vw); err != nil {
			log.Warn().Str("data", string(line)).Msgf("stdout forwarding")
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case ct.errChan <- &vw:
		}
		return nil
	})
}

func (ct *CustomTransformer) getConfigStdoutReader(ctx context.Context, stdout io.Reader) error {
	ct.outChan = make(chan []byte, 1)
	defer close(ct.outChan)
	return lineReader(ctx, stdout, func(line []byte) error {
		var vw *TransformerSettings
		if err := json.Unmarshal(line, &vw); err != nil {
			log.Warn().Str("data", string(line)).Msgf("stdout forwarding")
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case ct.settingsChan <- vw:
		}
		return nil
	})
}

func (ct *CustomTransformer) validate(ctx context.Context) (domains.ValidationWarnings, error) {
	var res domains.ValidationWarnings
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	eg, gtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
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
			case re, ok := <-ct.errChan:
				if !ok {
					return nil
				}
				res = append(res, re)
			}
		}
	})

	eg.Go(func() error {
		for {
			select {
			case <-gtx.Done():
				return gtx.Err()
			case data, ok := <-ct.outChan:
				if !ok {
					return nil
				}
				if len(data) > 0 {
					log.Warn().Str("data", string(data)).Msg("stdout forwarding")
				}
			}
		}
	})

	if err := eg.Wait(); err != nil && len(res) == 0 {
		return nil, err
	}
	return res, nil
}

func (ct *CustomTransformer) sendOriginalTuple(data []byte) error {
	select {
	case ct.inChan <- data:
	case <-ct.gtx.Done():
		return ct.gtx.Err()
	}

	return nil
}

func (ct *CustomTransformer) receiveTransformedTuple() ([]byte, error) {
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
