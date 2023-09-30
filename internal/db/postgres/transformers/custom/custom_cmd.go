package custom

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"io"
	"os/exec"
	"time"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

const (
	ValidateArgName    = "--validate"
	PrintConfigArgName = "--print-config"
	MetaArgName        = "--meta"
	ValidationTimeout  = 20 * time.Second
)

type CancelFunction func() error
type ReaderFunction func(ctx context.Context, r io.Reader) error
type WriterFunction func(ctx context.Context, r io.Writer) error

func ProduceNewCmdTransformerFunction(name string, executable string, cmd []string) toolkit.NewTransformerFunc {
	return func(
		ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
	) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
		return NewCmdTransformer(ctx, driver, parameters, name, executable, cmd)
	}
}

type CmdTransformer struct {
	name            string
	executable      string
	args            []string
	cmd             *exec.Cmd
	inChan          chan []byte
	outChan         chan []byte
	errChan         chan *toolkit.ValidationWarning
	settingsChan    chan *toolkit.CustomTransformerDefinition
	eg              *errgroup.Group
	gtx             context.Context
	cancel          CancelFunction
	driver          *toolkit.Driver
	parameters      map[string]*toolkit.Parameter
	affectedColumns []string
}

func NewCmdTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
	name string, executable string, args []string,
) (*CmdTransformer, toolkit.ValidationWarnings, error) {
	var affectedColumns []string
	for _, p := range parameters {
		if p.IsColumn {
			v := p.Value()
			columnName, ok := v.(*string)
			if !ok {
				return nil, nil, fmt.Errorf("unable to perform cast of column parameter value from any to *string")
			}
			affectedColumns = append(affectedColumns, *columnName)
		}
	}

	ct := &CmdTransformer{
		executable:      executable,
		args:            args,
		driver:          driver,
		parameters:      parameters,
		affectedColumns: affectedColumns,
		name:            name,
	}

	warnings, err := ct.Validate(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error validating transformer: %w", err)
	}

	return ct, warnings, fmt.Errorf("is not implemented")
}

func (ct *CmdTransformer) init(ctx context.Context, args []string,
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
			log.Warn().
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Msgf("error in closing function: %s", err.Error())
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

func (ct *CmdTransformer) getMetadata() (string, error) {
	var src []byte
	if err := json.Unmarshal(src, ct.driver.Table); err != nil {
		return "", fmt.Errorf("cannot unmarshal metadata: %w", err)
	}
	dst := make([]byte, 0, len(src))
	base64.StdEncoding.Encode(src, dst)
	return string(dst), nil
}

func (ct *CmdTransformer) Init(ctx context.Context) (err error) {
	// TODO: Generate table meta and pass it through the parameter encoded by base64
	meta, err := ct.getMetadata()
	overridenArgs := append(ct.args[0:], MetaArgName, meta)
	if err != nil {
		return fmt.Errorf("cannot get metatda: %w", err)
	}
	ct.cancel, err = ct.init(ctx, overridenArgs, ct.lineStdinWriter, ct.transformationStdoutReader, ct.transformationStderrReader)
	return err
}

func (ct *CmdTransformer) Done(ctx context.Context) (err error) {
	log.Debug().
		Str("TableSchema", ct.driver.Table.Schema).
		Str("TableName", ct.driver.Table.Name).
		Str("TransformerName", ct.name).
		Msg("terminating custom transformer")
	return ct.cancel()
}

func (ct *CmdTransformer) lineStdinWriter(ctx context.Context, stdin io.Writer) error {
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

func (ct *CmdTransformer) transformationStderrReader(ctx context.Context, stdout io.Reader) error {
	return lineReader(ctx, stdout, func(line []byte) error {
		log.Warn().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Str("Data", string(line)).
			Msg("stderr forwarding")
		return nil
	})
}

func (ct *CmdTransformer) transformationStdoutReader(ctx context.Context, stdout io.Reader) error {
	ct.settingsChan = make(chan *toolkit.CustomTransformerDefinition)
	defer close(ct.settingsChan)
	return lineReader(ctx, stdout, func(line []byte) error {
		var ts = &toolkit.CustomTransformerDefinition{}
		if err := json.Unmarshal(line, ts); err != nil {
			log.Warn().
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Str("Data", string(line)).
				Msg("stdout forwarding")
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

func (ct *CmdTransformer) validationStderrReader(ctx context.Context, stdout io.Reader) error {
	return lineReader(ctx, stdout, func(line []byte) error {
		log.Warn().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Str("Data", string(line)).
			Msg("stderr forwarding")
		return nil
	})
}

func (ct *CmdTransformer) validationStdoutReader(ctx context.Context, stdout io.Reader) error {
	ct.errChan = make(chan *toolkit.ValidationWarning, 10)
	defer close(ct.errChan)
	return lineReader(ctx, stdout, func(line []byte) error {
		var vw toolkit.ValidationWarning
		if err := json.Unmarshal(line, &vw); err != nil {
			log.Warn().
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Str("Data", string(line)).
				Msg("stdout forwarding")
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

func (ct *CmdTransformer) validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	var res toolkit.ValidationWarnings
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
					log.Warn().
						Str("TableSchema", ct.driver.Table.Schema).
						Str("TableName", ct.driver.Table.Name).
						Str("TransformerName", ct.name).
						Str("Data", string(data)).
						Msg("stdout forwarding")
				}
			}
		}
	})

	if err := eg.Wait(); err != nil && len(res) == 0 {
		return nil, err
	}
	return res, nil
}

func (ct *CmdTransformer) sendOriginalTuple(data []byte) error {
	select {
	case ct.inChan <- data:
	case <-ct.gtx.Done():
		return ct.gtx.Err()
	}

	return nil
}

func (ct *CmdTransformer) receiveTransformedTuple() ([]byte, error) {
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

func (ct *CmdTransformer) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	// TODO: Depending on transformer setting we can either validate or not. Ensure this logic has been implemented
	meta, err := ct.getMetadata()
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

func (ct *CmdTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	rrd, err := r.GetRawRecordDto(ct.affectedColumns...)
	if err != nil {
		return nil, fmt.Errorf("error gettings RawRecordDto: %w", err)
	}
	data, err := json.Marshal(rrd)
	if err != nil {
		return nil, fmt.Errorf("error marshaling RawRecordDto: %w", err)
	}

	if err = ct.sendOriginalTuple(data); err != nil {
		return nil, fmt.Errorf("cannot send tuple to transformer: %w", err)
	}

	data, err = ct.receiveTransformedTuple()
	if err != nil {
		return nil, fmt.Errorf("cannot receive transformerd tuple from transformer: %w", err)
	}

	trrd := make(toolkit.RawRecordDto)
	if err = json.Unmarshal(data, &trrd); err != nil {
		return nil, fmt.Errorf("error unmarshalling RawRecordDto")
	}

	if err = r.SetRawRecordDto(trrd); err != nil {
		return nil, fmt.Errorf("error setting RawRecordDto")
	}

	return r, nil
}
