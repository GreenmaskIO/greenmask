package custom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"io"
	"os/exec"
	"slices"
	"strings"
	"syscall"
)

const (
	ValidateArgName    = "--validate"
	PrintConfigArgName = "--print-config"
	MetaArgName        = "--meta"
)

type CancelFunction func() error
type ReaderFunction func(ctx context.Context, r io.ReadCloser) error
type WriterFunction func(ctx context.Context, r io.WriteCloser) error

func ProduceNewCmdTransformerFunction(ctd *toolkit.CustomTransformerDefinition) toolkit.NewTransformerFunc {
	return func(
		ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
	) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
		return NewCmdTransformer(ctx, driver, parameters, ctd)
	}
}

type CustomCmdTransformer struct {
	name            string
	executable      string
	args            []string
	cmd             *exec.Cmd
	inChan          chan []byte
	outChan         chan []byte
	warnings        []*toolkit.ValidationWarning
	eg              *errgroup.Group
	gtx             context.Context
	cancel          CancelFunction
	driver          *toolkit.Driver
	parameters      map[string]*toolkit.Parameter
	affectedColumns map[int]string
	ctd             *toolkit.CustomTransformerDefinition
}

func NewCmdTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
	ctd *toolkit.CustomTransformerDefinition,
) (*CustomCmdTransformer, toolkit.ValidationWarnings, error) {
	affectedColumns := make(map[int]string)
	for _, p := range parameters {
		if p.IsColumn {
			v := p.Value()
			columnName, ok := v.(*string)
			if !ok {
				return nil, nil, fmt.Errorf("unable to perform cast of column parameter value from any to *string")
			}
			idx := slices.IndexFunc(driver.Table.Columns, func(column *toolkit.Column) bool {
				return column.Name == *columnName
			})
			if idx == -1 {
				return nil, nil, fmt.Errorf("column with name %s is not found", *columnName)
			}
			affectedColumns[idx] = *columnName
		}
	}

	ct := &CustomCmdTransformer{
		executable:      ctd.Executable,
		args:            ctd.Args,
		driver:          driver,
		parameters:      parameters,
		affectedColumns: affectedColumns,
		name:            ctd.Name,
		ctd:             ctd,
	}

	var warnings toolkit.ValidationWarnings
	var err error
	if ctd.Validate {
		warnings, err = ct.Validate(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("error validating transformer: %w", err)
		}
	}

	return ct, warnings, nil
}

func (ct *CustomCmdTransformer) Init(ctx context.Context) (err error) {
	// TODO: Generate table meta and pass it through the parameter encoded by base64
	meta, err := ct.getMetadata()
	args := make([]string, len(ct.args))
	args = append(args, MetaArgName, meta)
	if err != nil {
		return fmt.Errorf("cannot get metatda: %w", err)
	}
	ct.cancel, err = ct.init(ctx, args, ct.lineStdinWriter, ct.transformationStdoutReader, ct.transformationStderrReader)

	if err != nil {
		return err
	}

	ct.eg.Go(func() error {
		if err := ct.cmd.Wait(); err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				if exitErr.ExitCode() != ct.ctd.ExpectedExitCode {
					log.Warn().
						Str("TableSchema", ct.driver.Table.Schema).
						Str("TableName", ct.driver.Table.Name).
						Str("TransformerName", ct.name).
						Str("TransformerName", ct.name).
						Int("TransformerExitCode", ct.cmd.ProcessState.ExitCode()).
						Msg("unexpected exit code")
					return fmt.Errorf("unexpeted transformer exit code: exepected %d received %d",
						ct.ctd.ExpectedExitCode, ct.cmd.ProcessState.ExitCode())
				}
				return err
			} else {
				log.Error().
					Err(err).
					Str("TableSchema", ct.driver.Table.Schema).
					Str("TableName", ct.driver.Table.Name).
					Str("TransformerName", ct.name).
					Int("TransformerPid", ct.cmd.Process.Pid).
					Msg("custom transformer exited with error")
				return fmt.Errorf("transformer exited with error: %w", err)
			}
		}

		log.Debug().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Int("TransformerPid", ct.cmd.Process.Pid).
			Msg("transformer exited normally")
		return nil
	})

	return nil
}

func (ct *CustomCmdTransformer) Done(ctx context.Context) (err error) {
	log.Debug().
		Str("TableSchema", ct.driver.Table.Schema).
		Str("TableName", ct.driver.Table.Name).
		Str("TransformerName", ct.name).
		Msg("terminating custom transformer")

	if err := ct.cancel(); err != nil {
		log.Debug().
			Err(err).
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Msg("error in termination function")
		return fmt.Errorf("error terminating custom transformer: %w", err)
	}

	if err := ct.eg.Wait(); err != nil {
		log.Warn().
			Err(err).
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Msg("one of custom transformer goroutine exited with error")
		return fmt.Errorf("one of custom transformer goroutine exited with error: %w", err)
	}
	log.Debug().
		Str("TableSchema", ct.driver.Table.Schema).
		Str("TableName", ct.driver.Table.Name).
		Str("TransformerName", ct.name).
		Msg("terminated successfully")
	return nil
}

func (ct *CustomCmdTransformer) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	// TODO: Depending on transformer setting we can either validate or not. Ensure this logic has been implemented
	meta, err := ct.getMetadata()
	args := make([]string, len(ct.args))
	copy(args, ct.args)
	args = append(args, ValidateArgName, MetaArgName, meta)
	cancelFunction, err := ct.init(ctx, args, nil, ct.validationStdoutReader, ct.validationStderrReader)
	if err != nil {
		return nil, fmt.Errorf("transformer initialisation error: %w", err)
	}

	ct.eg.Go(func() error {
		defer cancelFunction()
		if err := ct.cmd.Wait(); err != nil {
			log.Error().
				Err(err).
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Int("TransformerPid", ct.cmd.Process.Pid).
				Msg("custom transformer exited with error")
			return fmt.Errorf("transformer exited with error: %w", err)
		}
		log.Debug().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Int("TransformerPid", ct.cmd.Process.Pid).
			Msg("transformer exited normally")
		return nil
	})

	ctx, cancel := context.WithTimeout(ctx, ct.ctd.ValidationTimeout)
	defer cancel()

	warnings, err := ct.validate(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot perform transformer validation: %w", err)
	}

	return warnings, nil
}

func (ct *CustomCmdTransformer) init(ctx context.Context, args []string,
	stdinWriterFunc WriterFunction, stdoutReaderFunc ReaderFunction, stderrReaderFunc ReaderFunction,
) (CancelFunction, error) {
	log.Debug().
		Str("executable", ct.executable).
		Str("args", strings.Join(args, " ")).
		Msg("running custom transformer")

	if stderrReaderFunc == nil || stdoutReaderFunc == nil {
		return nil, errors.New("stderrReaderFunc and stdoutReaderFunc cannot be nil")
	}

	ctx, cancel := context.WithCancel(ctx)
	ct.cmd = exec.Command(ct.executable, args...)

	stderr, err := ct.cmd.StderrPipe()
	if err != nil {
		cancel()
	}
	stdout, err := ct.cmd.StdoutPipe()
	if err != nil {
		stdout.Close()
		cancel()
	}

	stdin, err := ct.cmd.StdinPipe()
	if err != nil {
		cancel()
		stderr.Close()
		stdout.Close()
	}

	log.Debug().Msgf("stdin = %v", stdin)
	log.Debug().Msgf("stdout = %v", stdout)
	log.Debug().Msgf("stderr = %v", stderr)

	ct.inChan = make(chan []byte)

	ct.eg, ct.gtx = errgroup.WithContext(ctx)

	cancelFunction := func() error {
		log.Debug().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Msg("running closing function")

		cancel()

		if ct.cmd.Process != nil && ct.cmd.ProcessState == nil ||
			ct.cmd.Process != nil && ct.cmd.ProcessState != nil && !ct.cmd.ProcessState.Exited() {
			log.Debug().
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Str("TransformerName", ct.name).
				Int("TransformerPid", ct.cmd.Process.Pid).
				Msg("sending SIGTERM to custom transformer process")
			if err := ct.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				log.Debug().
					Err(err).
					Str("TableSchema", ct.driver.Table.Schema).
					Str("TableName", ct.driver.Table.Name).
					Str("TransformerName", ct.name).
					Int("TransformerPid", ct.cmd.Process.Pid).
					Msg("error sending SIGTERM to custom transformer process")

				if ct.cmd.ProcessState != nil && !ct.cmd.ProcessState.Exited() {
					log.Warn().
						Str("TableSchema", ct.driver.Table.Schema).
						Str("TableName", ct.driver.Table.Name).
						Str("TransformerName", ct.name).
						Int("TransformerPid", ct.cmd.Process.Pid).
						Msg("killing process")
					if err = ct.cmd.Process.Kill(); err != nil {
						log.Warn().
							Err(err).
							Int("pid", ct.cmd.Process.Pid).
							Msg("error terminating custom transformer process")
					}
				}
			}
		}
		close(ct.inChan)

		log.Debug().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Str("TransformerName", ct.name).
			Msg("closing function completed successfully")

		return nil
	}

	if err := ct.cmd.Start(); err != nil {
		cancelFunction()
		event := log.Warn().
			Err(err).
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name)
		if ct.cmd.Process != nil {
			event.Int("TransformerPid", ct.cmd.Process.Pid)
		}
		event.Msg("custom transformer exited with error")
		return nil, fmt.Errorf("external command runtime error: %w", err)
	}

	ct.eg.Go(func() error {
		return stderrReaderFunc(ct.gtx, stderr)
	})

	ct.eg.Go(func() error {
		return stdoutReaderFunc(ct.gtx, stdout)
	})

	if stdinWriterFunc != nil {
		ct.eg.Go(func() error {
			defer func() {
				log.Debug().Msg("closing stdinWriter")
				if err := stdin.Close(); err != nil {
					log.Debug().
						Str("TableSchema", ct.driver.Table.Schema).
						Str("TableName", ct.driver.Table.Name).
						Str("TransformerName", ct.name).
						Str("TransformerName", ct.name).
						Err(err).
						Msg("error closing stdin")
				}
			}()
			return stdinWriterFunc(ct.gtx, stdin)
		})
	}

	return cancelFunction, nil
}

func (ct *CustomCmdTransformer) validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	ctx, cancel := context.WithTimeout(ctx, ct.ctd.ValidationTimeout)
	defer cancel()

	if err := ct.eg.Wait(); err != nil {
		return nil, err
	}
	return ct.warnings, nil
}

func (ct *CustomCmdTransformer) getMetadata() (string, error) {
	res, err := json.Marshal(&ct.driver.Table)
	if err != nil {
		return "", fmt.Errorf("cannot marshal metadata: %w", err)
	}
	return string(res), nil
}

func (ct *CustomCmdTransformer) lineStdinWriter(ctx context.Context, stdin io.WriteCloser) error {

	for {
		select {
		case data, ok := <-ct.inChan:
			if !ok {
				log.Debug().
					Str("TransformerName", ct.ctd.Name).
					Int("TransformerPid", ct.cmd.Process.Pid).
					Str("TableSchema", ct.driver.Table.Schema).
					Str("TableName", ct.driver.Table.Name).
					Msg("lineStdinWriter exited because channel was closed")
				return nil
			}
			_, err := stdin.Write(data)
			if err != nil {
				log.Warn().
					Err(err).
					Str("TransformerName", ct.ctd.Name).
					Int("TransformerPid", ct.cmd.Process.Pid).
					Str("TableSchema", ct.driver.Table.Schema).
					Str("TableName", ct.driver.Table.Name).
					Msg("cannot send data to stdin of transformer")
				return fmt.Errorf("error sending data to stdin: %w", err)
			}
		case <-ctx.Done():
			log.Debug().Msg("closed")
			return nil
		}
	}
}

func (ct *CustomCmdTransformer) transformationStderrReader(ctx context.Context, stderr io.ReadCloser) error {
	return lineReader(ctx, stderr, func(line []byte) error {
		log.Warn().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Int("TransformerPid", ct.cmd.Process.Pid).
			Str("Data", string(line)).
			Msg("stderr forwarding")
		return nil
	})
}

func (ct *CustomCmdTransformer) transformationStdoutReader(ctx context.Context, stdout io.ReadCloser) error {
	ct.outChan = make(chan []byte)
	defer close(ct.outChan)
	return lineReader(ctx, stdout, func(line []byte) error {
		select {
		case ct.outChan <- line:
			return nil
		case <-ctx.Done():
			log.Debug().Msg("closed")
			return nil
		}
	})
}

func (ct *CustomCmdTransformer) validationStderrReader(ctx context.Context, stderr io.ReadCloser) error {
	return lineReader(ctx, stderr, func(line []byte) error {
		log.Warn().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Int("TransformerPid", ct.cmd.Process.Pid).
			Str("Data", string(line)).
			Msg("stderr forwarding")
		return nil
	})
}

func (ct *CustomCmdTransformer) validationStdoutReader(ctx context.Context, stdout io.ReadCloser) error {

	return lineReader(ctx, stdout, func(line []byte) error {
		vw := toolkit.NewValidationWarning()
		if err := json.Unmarshal(line, &vw); err != nil {
			log.Warn().
				Err(err).
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Int("TransformerPid", ct.cmd.Process.Pid).
				Str("Data", string(line)).
				Msg("error unmarshalling ValidationWarning")
			return fmt.Errorf("error unmarshalling ValidationWarning: %w", err)
		}
		ct.warnings = append(ct.warnings, vw)
		return nil
	})
}

func (ct *CustomCmdTransformer) sendOriginalTuple(ctx context.Context, data []byte) error {
	select {
	case ct.inChan <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-ct.gtx.Done():
		return ct.gtx.Err()
	}
}

func (ct *CustomCmdTransformer) receiveTransformedTuple(ctx context.Context) ([]byte, error) {
	select {
	case data, ok := <-ct.outChan:
		if !ok {
			log.Warn().
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Int("TransformerPid", ct.cmd.Process.Pid).
				Msg("channel unexpectedly closed")
			return nil, fmt.Errorf("channel unexpectedly closed")
		}
		if len(data) == 0 {
			log.Warn().
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Int("TransformerPid", ct.cmd.Process.Pid).
				Str("Data", string(data)).
				Msg("received empty tuple after transformation")
			return nil, fmt.Errorf("received empty tuple after transformation")
		}
		return data, nil
	case <-ct.gtx.Done():
		log.Debug().Msg("closed")
		return nil, ct.gtx.Err()
	case <-ctx.Done():
		log.Debug().Msg("closed")
		return nil, ctx.Err()
	}
}

func (ct *CustomCmdTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, ct.ctd.RowTransformationTimeout)
	defer cancel()
	rrd, err := GetRawRecordDto(r, ct.affectedColumns)
	if err != nil {
		return nil, fmt.Errorf("error gettings RawRecordDto: %w", err)
	}
	originalData, err := json.Marshal(rrd)
	if err != nil {
		return nil, fmt.Errorf("error marshaling RawRecordDto: %w", err)
	}
	originalData = append(originalData, '\n')

	if err = ct.sendOriginalTuple(ctx, originalData); err != nil {
		return nil, fmt.Errorf("cannot send tuple to transformer: %w", err)
	}

	transformedData, err := ct.receiveTransformedTuple(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot receive transformed tuple from transformer: %w", err)
	}

	trrd := make(toolkit.RawRecordDto)
	if err = json.Unmarshal(transformedData, &trrd); err != nil {
		return nil, fmt.Errorf("error unmarshalling RawRecordDto")
	}

	if err = SetRawRecordDto(r, trrd); err != nil {
		return nil, fmt.Errorf("error setting RawRecordDto")
	}

	return r, nil
}
