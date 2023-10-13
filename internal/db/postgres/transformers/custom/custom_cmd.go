package custom

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var (
	ErrValidationTimeout        = errors.New("validation timeout")
	ErrRowTransformationTimeout = errors.New("row transformation timeout")
)

const (
	ValidateArgName        = "--validate"
	PrintDefinitionArgName = "--print-definition"
	MetaArgName            = "--meta"
	TransformArgName       = "--transform"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type CancelFunction func() error

func ProduceNewCmdTransformerFunction(ctd *utils.CustomTransformerDefinition) utils.NewTransformerFunc {
	return func(
		ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
	) (utils.Transformer, toolkit.ValidationWarnings, error) {
		return NewCustomCmdTransformer(ctx, driver, parameters, ctd)
	}
}

type CustomCmdTransformer struct {
	name               string
	executable         string
	args               []string
	cmd                *exec.Cmd
	stdoutReader       *bufio.Reader
	stderrReader       *bufio.Reader
	stdinWriter        io.Writer
	warnings           []*toolkit.ValidationWarning
	eg                 *errgroup.Group
	gtx                context.Context
	cancel             CancelFunction
	driver             *toolkit.Driver
	parameters         map[string]*toolkit.Parameter
	affectedColumns    map[int]string
	ctd                *utils.CustomTransformerDefinition
	sendChan           chan struct{}
	receiveChan        chan struct{}
	t                  *time.Ticker
	skipOriginalData   bool
	skipTransformation bool
}

func NewCustomCmdTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
	ctd *utils.CustomTransformerDefinition,
) (*CustomCmdTransformer, toolkit.ValidationWarnings, error) {
	var skipOriginalData bool
	affectedColumns := make(map[int]string)
	for _, p := range parameters {
		if p.IsColumn {
			v, err := p.Value()
			if err != nil {
				return nil, nil, fmt.Errorf("error getting parameter value: %w", err)
			}
			columnName, ok := v.(string)
			if !ok {
				return nil, nil, fmt.Errorf("unable to perform cast of column parameter value from any to *string")
			}
			idx := slices.IndexFunc(driver.Table.Columns, func(column *toolkit.Column) bool {
				return column.Name == columnName
			})
			if idx == -1 {
				return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
			}
			affectedColumns[idx] = columnName
			if p.ColumnProperties != nil && p.ColumnProperties.SkipOriginalData {
				skipOriginalData = true
			}
		}
	}

	ct := &CustomCmdTransformer{
		executable:       ctd.Executable,
		args:             ctd.Args,
		driver:           driver,
		parameters:       parameters,
		affectedColumns:  affectedColumns,
		name:             ctd.Name,
		ctd:              ctd,
		skipOriginalData: skipOriginalData,
		t:                time.NewTicker(ctd.RowTransformationTimeout),
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

func (ct *CustomCmdTransformer) GetAffectedColumns() map[int]string {
	return ct.affectedColumns
}

func (ct *CustomCmdTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	ct.t.Reset(ct.ctd.RowTransformationTimeout)
	var rrd toolkit.RawRecord
	var err error
	if !ct.skipOriginalData {
		rrd, err = GetRawRecordDto(r, ct.affectedColumns)
		if err != nil {
			return nil, fmt.Errorf("error gettings RawRecordDto: %w", err)
		}
	}

	if err = ct.sendOriginalTuple(ctx, rrd); err != nil {
		return nil, fmt.Errorf("cannot send tuple to transformer: %w", err)
	}

	transformedData, err := ct.receiveTransformedTuple(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot receive transformed tuple from transformer: %w", err)
	}

	trrd := make(toolkit.RawRecord, len(ct.driver.Table.Columns))
	if err = json.Unmarshal(transformedData, &trrd); err != nil {
		return nil, fmt.Errorf("error unmarshalling RawRecordDto: unexpected record format: %w", err)
	}

	if err = SetRawRecordDto(r, trrd); err != nil {
		return nil, fmt.Errorf("error setting RawRecordDto")
	}

	return r, nil
}

func (ct *CustomCmdTransformer) Init(ctx context.Context) (err error) {
	// TODO: Generate table meta and pass it through the parameter encoded by base64
	meta, err := ct.getMetadata()
	args := make([]string, len(ct.args))
	args = append(args, MetaArgName, meta, TransformArgName)
	if err != nil {
		return fmt.Errorf("cannot get metatda: %w", err)
	}
	ct.cancel, err = ct.init(ctx, args)

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

	ctx, cancel := context.WithTimeout(ctx, ct.ctd.ValidationTimeout)
	defer cancel()
	_, err = ct.init(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("transformer initialisation error: %w", err)
	}

	ct.eg.Go(func() error {
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

	var warnings toolkit.ValidationWarnings

	for {
		line, _, err := ct.stdoutReader.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
				break
			}
			log.Debug().Err(err).Msg("line reader error")
			return nil, err
		}

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
			return nil, fmt.Errorf("error unmarshalling ValidationWarning: %w", err)
		}
		warnings = append(warnings, vw)
	}

	if err = ct.eg.Wait(); err != nil {
		if ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			log.Warn().
				Err(err).
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Int("TransformerPid", ct.cmd.Process.Pid).
				Dur("ValidationTimeout", ct.ctd.ValidationTimeout).
				Msg("validation timeout")
			return nil, ErrValidationTimeout
		}
		return nil, err
	}

	return warnings, nil
}

func (ct *CustomCmdTransformer) init(ctx context.Context, args []string) (CancelFunction, error) {
	log.Debug().
		Str("executable", ct.executable).
		Str("args", strings.Join(args, " ")).
		Msg("running custom transformer")

	ct.cmd = exec.CommandContext(ctx, ct.executable, args...)
	ct.sendChan = make(chan struct{}, 1)
	ct.receiveChan = make(chan struct{}, 1)

	var err error
	stderr, err := ct.cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := ct.cmd.StdoutPipe()
	if err != nil {
		stdout.Close()
		return nil, err
	}

	stdin, err := ct.cmd.StdinPipe()
	if err != nil {
		stderr.Close()
		stdout.Close()
		return nil, err
	}
	ct.stderrReader = bufio.NewReader(stderr)
	ct.stdoutReader = bufio.NewReader(stdout)
	ct.stdinWriter = stdin

	cancelFunction := func() error {
		log.Debug().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Msg("running closing function")

		if ct.cmd.Process != nil && ct.cmd.ProcessState == nil ||
			ct.cmd.Process != nil && ct.cmd.ProcessState != nil && !ct.cmd.ProcessState.Exited() {
			log.Debug().
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
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

		if err := stdin.Close(); err != nil {
			log.Debug().
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Err(err).
				Msg("error closing stdin")
		}

		log.Debug().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Msg("closing function completed successfully")

		return nil
	}

	ct.cmd.Cancel = cancelFunction

	ct.eg = &errgroup.Group{}
	ct.eg.Go(func() error {
		return ct.stderrForwarder(ctx, ct.stderrReader)
	})

	if err := ct.cmd.Start(); err != nil {
		log.Warn().
			Err(err).
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Msg("custom transformer exited with error")

		return nil, fmt.Errorf("external command runtime error: %w", err)
	}

	return cancelFunction, nil
}

func (ct *CustomCmdTransformer) getMetadata() (string, error) {
	params := make(map[string]toolkit.ParamsValue)
	for name, p := range ct.parameters {
		params[name] = p.RawValue()
	}
	meta := &toolkit.Meta{
		Table:      ct.driver.Table,
		Parameters: params,
		Types:      ct.driver.CustomTypes,
	}
	res, err := json.Marshal(&meta)
	if err != nil {
		return "", fmt.Errorf("cannot marshal metadata: %w", err)
	}
	return string(res), nil
}

func (ct *CustomCmdTransformer) stderrForwarder(ctx context.Context, stderr io.Reader) error {
	lineScanner := bufio.NewReader(stderr)
	for {

		line, _, err := lineScanner.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
				return nil
			}
			log.Debug().Err(err).Msg("line reader error")
			return err
		}

		log.Warn().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Int("TransformerPid", ct.cmd.Process.Pid).
			Msg("stderr forwarding")
		fmt.Printf("\tDATA: %s\n", string(line))

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (ct *CustomCmdTransformer) sendOriginalTuple(ctx context.Context, rawRecord toolkit.RawRecord) (err error) {
	go func() {
		if rawRecord == nil {
			_, err = ct.stdinWriter.Write([]byte{'\n'})
		} else {
			err = json.NewEncoder(ct.stdinWriter).Encode(rawRecord)
		}

		ct.sendChan <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ct.t.C:
		return ErrRowTransformationTimeout
	case <-ct.sendChan:
	}
	if err != nil {
		return err
	}
	return nil
}

func (ct *CustomCmdTransformer) receiveTransformedTuple(ctx context.Context) (line []byte, err error) {
	go func() {
		line, _, err = ct.stdoutReader.ReadLine()
		ct.receiveChan <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ct.t.C:
		return nil, ErrRowTransformationTimeout
	case <-ct.receiveChan:
	}

	if err != nil {
		return nil, fmt.Errorf("error receiving data from transformer: %w", err)
	}
	return line, nil
}
