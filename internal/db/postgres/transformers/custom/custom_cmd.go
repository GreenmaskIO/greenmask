// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

func ProduceNewCmdTransformerFunction(ctd *TransformerDefinition) utils.NewTransformerFunc {
	return func(
		ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
	) (utils.Transformer, toolkit.ValidationWarnings, error) {
		return NewCustomCmdTransformer(ctx, driver, parameters, ctd)
	}
}

type CmdTransformer struct {
	*utils.CmdTransformerBase

	name            string
	executable      string
	args            []string
	eg              *errgroup.Group
	driver          *toolkit.Driver
	parameters      map[string]*toolkit.Parameter
	affectedColumns map[int]string
	ctd             *TransformerDefinition
	t               *time.Ticker
}

func NewCustomCmdTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
	ctd *TransformerDefinition,
) (*CmdTransformer, toolkit.ValidationWarnings, error) {
	affectedColumns := make(map[int]string)
	affectedColumnsIdx, transferringColumnsIdx, err := toolkit.GetAffectedAndTransferringColumns(parameters, driver)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting affeected and transferring columns: %w", err)
	}
	for _, idx := range affectedColumnsIdx {
		affectedColumns[idx] = driver.Table.Columns[idx].Name
	}

	api, err := toolkit.NewApi(ctd.Driver, transferringColumnsIdx, affectedColumnsIdx, driver)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating InteractionApi: %w", err)
	}

	cct := utils.NewCmdTransformerBase(ctd.Name, ctd.ExpectedExitCode, driver, api)

	ct := &CmdTransformer{
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

func (ct *CmdTransformer) GetAffectedColumns() map[int]string {
	return ct.affectedColumns
}

func (ct *CmdTransformer) watchForTimeout(ctx context.Context) error {
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
				if err := ct.Cancel(); err != nil {
					log.Warn().Err(err).Msg("error closing transformer")
				}
				return ErrRowTransformationTimeout
			}
		}
	}
}

func (ct *CmdTransformer) Init(ctx context.Context) (err error) {
	// TODO: Generate table meta and pass it through the parameter encoded by base64
	meta, err := ct.getMetadata()
	if err != nil {
		return fmt.Errorf("error getting metadata: %w", err)
	}
	log.Debug().
		RawJSON("Meta", []byte(meta)).
		Str("TableSchema", ct.driver.Table.Schema).
		Str("TableName", ct.driver.Table.Name).
		Str("TransformerName", ct.name).
		Msg("running transformer with metadata")

	args := make([]string, len(ct.args))
	copy(args, ct.args)
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

func (ct *CmdTransformer) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	// TODO: Depending on transformer setting we can either validate or not. Ensure this logic has been implemented
	meta, err := ct.getMetadata()
	if err != nil {
		return nil, fmt.Errorf("error getting metadata: %w", err)
	}

	log.Debug().
		RawJSON("Meta", []byte(meta)).
		Str("TableSchema", ct.driver.Table.Schema).
		Str("TableName", ct.driver.Table.Name).
		Str("TransformerName", ct.name).
		Msg("running transformer with metadata")
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

func (ct *CmdTransformer) Done(ctx context.Context) error {
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

func (ct *CmdTransformer) getMetadata() (string, error) {
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
	//`{"9":{"d":null,"n":true}}\n`
}

func (ct *CmdTransformer) stderrForwarder(ctx context.Context) error {
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
