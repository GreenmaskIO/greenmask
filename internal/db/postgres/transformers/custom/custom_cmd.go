package custom

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
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

var json = jsoniter.ConfigFastest

func ProduceNewCmdTransformerFunction(ctd *CustomTransformerDefinition) utils.NewTransformerFunc {
	return func(
		ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
	) (utils.Transformer, toolkit.ValidationWarnings, error) {
		return NewCustomCmdTransformer(ctx, driver, parameters, ctd)
	}
}

type CustomCmdTransformer struct {
	*utils.CmdTransformerBase

	name               string
	executable         string
	args               []string
	warnings           []*toolkit.ValidationWarning
	eg                 *errgroup.Group
	driver             *toolkit.Driver
	parameters         map[string]*toolkit.Parameter
	affectedColumns    map[int]string
	ctd                *CustomTransformerDefinition
	t                  *time.Ticker
	skipTransformation bool
}

func NewCustomCmdTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
	ctd *CustomTransformerDefinition,
) (*CustomCmdTransformer, toolkit.ValidationWarnings, error) {
	affectedColumns := make(map[int]string)
	var affectedColumnsIdx []int
	var transferringColumnsIdx []int
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

			idx, _, ok := driver.GetColumnByName(columnName)
			if !ok {
				return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
			}
			if p.ColumnProperties != nil {
				if p.ColumnProperties.Affected {
					affectedColumns[idx] = columnName
					affectedColumnsIdx = append(affectedColumnsIdx, idx)
				}
			} else {
				affectedColumns[idx] = columnName
				affectedColumnsIdx = append(affectedColumnsIdx, idx)

			}

			if p.ColumnProperties != nil {
				if !p.ColumnProperties.SkipOriginalData {
					transferringColumnsIdx = append(transferringColumnsIdx, idx)
				}
			} else {
				transferringColumnsIdx = append(transferringColumnsIdx, idx)
			}
		}
	}

	api, err := utils.NewApi(ctd.Mode, transferringColumnsIdx, affectedColumnsIdx)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating InteractionApi: %w", err)
	}

	cct := utils.NewCmdTransformerBase(ctd.Name, ctd.ExpectedExitCode, driver, api)

	ct := &CustomCmdTransformer{
		CmdTransformerBase: cct,
		executable:         ctd.Executable,
		args:               ctd.Args,
		driver:             driver,
		parameters:         parameters,
		affectedColumns:    affectedColumns,
		name:               ctd.Name,
		ctd:                ctd,
		t:                  time.NewTicker(ctd.RowTransformationTimeout),
	}

	var warnings toolkit.ValidationWarnings
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

func (ct *CustomCmdTransformer) watchForTimeout(ctx context.Context) error {
	for {
		if ct.ProcessedLines > -1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ct.DoneCh:
			return nil
		default:
		}
		time.Sleep(1 * time.Second)
	}
	ct.t.Reset(ct.ctd.RowTransformationTimeout)
	for {
		lastValue := ct.ProcessedLines
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ct.DoneCh:
			return nil
		case <-ct.t.C:
			if lastValue == ct.ProcessedLines {
				ct.Cancel()
				return ErrRowTransformationTimeout
			}
		}
	}
}

func (ct *CustomCmdTransformer) Init(ctx context.Context) (err error) {
	// TODO: Generate table meta and pass it through the parameter encoded by base64
	meta, err := ct.getMetadata()
	args := make([]string, len(ct.args))
	args = append(args, MetaArgName, meta, TransformArgName)
	if err != nil {
		return fmt.Errorf("cannot get metatda: %w", err)
	}
	err = ct.BaseInit(ct.executable, args)

	if err != nil {
		return err
	}

	ct.eg = &errgroup.Group{}
	ct.eg.Go(func() error {
		return ct.stderrForwarder(ctx)
	})

	ct.eg.Go(func() error {
		return ct.watchForTimeout(ctx)
	})

	ct.eg.Go(func() error {
		if err := ct.Cmd.Wait(); err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				if exitErr.ExitCode() != ct.ctd.ExpectedExitCode {
					log.Warn().
						Str("TableSchema", ct.driver.Table.Schema).
						Str("TableName", ct.driver.Table.Name).
						Str("TransformerName", ct.name).
						Int("TransformerExitCode", ct.Cmd.ProcessState.ExitCode()).
						Msg("unexpected exit code")
					return fmt.Errorf("unexpeted transformer exit code: exepected %d received %d",
						ct.ctd.ExpectedExitCode, ct.Cmd.ProcessState.ExitCode())
				}
				return err
			} else {
				log.Error().
					Err(err).
					Str("TableSchema", ct.driver.Table.Schema).
					Str("TableName", ct.driver.Table.Name).
					Str("TransformerName", ct.name).
					Int("TransformerPid", ct.Cmd.Process.Pid).
					Msg("custom transformer exited with error")
				return fmt.Errorf("transformer exited with error: %w", err)
			}
		}

		log.Debug().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Int("TransformerPid", ct.Cmd.Process.Pid).
			Msg("transformer exited normally")
		return nil
	})

	return nil
}

func (ct *CustomCmdTransformer) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	// TODO: Depending on transformer setting we can either validate or not. Ensure this logic has been implemented
	meta, err := ct.getMetadata()
	args := make([]string, len(ct.args))
	copy(args, ct.args)
	args = append(args, ValidateArgName, MetaArgName, meta)

	ct.eg = &errgroup.Group{}
	ctx, cancel := context.WithTimeout(ctx, ct.ctd.ValidationTimeout)
	defer cancel()
	err = ct.BaseInitWithContext(ctx, ct.executable, args)
	if err != nil {
		return nil, fmt.Errorf("transformer initialisation error: %w", err)
	}

	ct.eg.Go(func() error {
		return ct.stderrForwarder(ctx)
	})

	ct.eg.Go(func() error {
		if err := ct.Cmd.Wait(); err != nil {
			log.Error().
				Err(err).
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Int("TransformerPid", ct.Cmd.Process.Pid).
				Msg("custom transformer exited with error")
			return fmt.Errorf("transformer exited with error: %w", err)
		}
		log.Debug().
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Int("TransformerPid", ct.Cmd.Process.Pid).
			Msg("transformer exited normally")
		return nil
	})

	var warnings toolkit.ValidationWarnings

	for {
		line, err := ct.ReceiveStdoutLine(ctx)
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
				Int("TransformerPid", ct.Cmd.Process.Pid).
				Str("Data", string(line)).
				Msg("error unmarshalling ValidationWarning")
			vw = toolkit.NewValidationWarning().
				AddMeta("Payload", string(line)).
				SetSeverity(toolkit.ErrorValidationSeverity).
				SetMsg("error unmarshalling validation warning")
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
				Int("TransformerPid", ct.Cmd.Process.Pid).
				Dur("ValidationTimeout", ct.ctd.ValidationTimeout).
				Msg("validation timeout")
			return nil, ErrValidationTimeout
		}
		return nil, err
	}

	return warnings, nil
}

func (ct *CustomCmdTransformer) Done(ctx context.Context) error {
	if err := ct.BaseDone(); err != nil {
		return err
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
	return nil
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

func (ct *CustomCmdTransformer) stderrForwarder(ctx context.Context) error {
	for {
		line, _, err := ct.StderrReader.ReadLine()
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
			Int("TransformerPid", ct.Cmd.Process.Pid).
			Msg("stderr forwarding")
		fmt.Printf("\tDATA: %s\n", string(line))

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}
