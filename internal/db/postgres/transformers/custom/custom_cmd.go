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
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var (
	ErrValidationTimeout = errors.New("validation timeout")
)

const (
	ValidateArgName        = "--validate"
	PrintDefinitionArgName = "--print-definition"
	TransformArgName       = "--transform"
)

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
	for _, c := range affectedColumnsIdx {
		affectedColumns[c.Idx] = c.Name
	}

	api, err := toolkit.NewApi(ctd.Driver, transferringColumnsIdx, affectedColumnsIdx, driver)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating InteractionApi: %w", err)
	}

	cct := utils.NewCmdTransformerBase(ctd.Name, ctd.ExpectedExitCode, ctd.RowTransformationTimeout, driver, api)

	ct := &CmdTransformer{
		CmdTransformerBase: cct,
		executable:         ctd.Executable,
		args:               ctd.Args,
		driver:             driver,
		parameters:         parameters,
		affectedColumns:    affectedColumns,
		name:               ctd.Name,
		ctd:                ctd,
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

func (ct *CmdTransformer) Init(ctx context.Context) (err error) {

	args := make([]string, len(ct.args))
	copy(args, ct.args)
	args = append(args, TransformArgName)
	if err != nil {
		return fmt.Errorf("cannot get metatda: %w", err)
	}
	err = ct.BaseInit(ct.executable, args)
	if err != nil {
		return err
	}

	if err = ct.sendMetadata(); err != nil {
		return fmt.Errorf("error sending metadata: %w", err)
	}

	ct.eg = &errgroup.Group{}
	ct.eg.Go(func() error {
		return ct.stderrForwarder(ctx)
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

	args := make([]string, len(ct.args))
	copy(args, ct.args)
	args = append(args, ValidateArgName)

	ct.eg = &errgroup.Group{}
	ctx, cancel := context.WithTimeout(ctx, ct.ctd.ValidationTimeout)
	defer cancel()
	err := ct.BaseInitWithContext(ctx, ct.executable, args)
	if err != nil {
		return nil, fmt.Errorf("transformer initialisation error: %w", err)
	}

	if err = ct.sendMetadata(); err != nil {
		return nil, fmt.Errorf("error sending metadata: %w", err)
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

func (ct *CmdTransformer) getMetadata() ([]byte, error) {
	params := make(toolkit.Params)
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
		return nil, fmt.Errorf("cannot marshal metadata: %w", err)
	}
	return res, nil
}

func (ct *CmdTransformer) stderrForwarder(ctx context.Context) error {
	t := time.NewTicker(500 * time.Millisecond)
	lineNum := 0
	// This is required for convenient verbosity of output.
	// Write "stderr forwarding" log message each 500ms otherwise just print received stderr data
	// If it does not use this logic each line would be covered with "stderr forwarding" message and it will be
	// complicated to recognize the traceback or multiline message
	for {
		line, _, err := ct.StderrReader.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
				return nil
			}
			log.Debug().Err(err).Msg("line reader error")
			return err
		}

		if lineNum == 0 {
			log.Warn().
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Int("TransformerPid", ct.Cmd.Process.Pid).
				Msg("stderr forwarding")
		}
		fmt.Printf("\tDATA: %s\n", string(line))

		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			lineNum = 0
		default:
		}
		lineNum++
	}
}

func (ct *CmdTransformer) sendMetadata() error {
	meta, err := ct.getMetadata()
	if err != nil {
		return err
	}
	log.Debug().
		RawJSON("Meta", meta).
		Str("TableSchema", ct.driver.Table.Schema).
		Str("TableName", ct.driver.Table.Name).
		Str("TransformerName", ct.name).
		Msg("running transformer with metadata")

	meta = append(meta, '\n')
	_, err = ct.StdinWriter.Write(meta)
	if err != nil {
		return fmt.Errorf("error writing metatda to stdin: %w", err)
	}
	return nil
}
