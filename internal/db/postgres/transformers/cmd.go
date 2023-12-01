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

package transformers

import (
	"context"
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

const (
	cmdRowDriverTextName = "text"
	cmdRowDriverJsonName = "json"
	cmdRowDriverCsvName  = "csv"
)

const (
	skipOnAny = iota
	skipOnAll
)

const (
	skipOnAnyName = "any"
	skipOnAllName = "all"
)

var cmdTransformerName = "Cmd"

var ErrRowTransformationTimeout = errors.New("row transformation timeout")

var CmdTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		cmdTransformerName,
		"Transform data via external program using stdin and stdout interaction",
	),

	NewCmd,

	toolkit.MustNewParameter(
		"columns",
		"affected column names. If empty use the whole tuple."+
			"The structure:"+
			`{`+
			`"name": "type:string, required:true, description: column Name",`+
			`"not_affected": "type:bool, required:false, description: is column affected in transformation", `+
			`"skip_original_data":  "type:bool, required:false, description: is original data required for transformer",`+
			`"skip_on_null_input":  "type:bool, required:false, description: skip transformation on null input"`+
			`}`,
	).SetDefaultValue([]byte("[]")),

	toolkit.MustNewParameter(
		"executable",
		"path to executable file",
	).SetRequired(true),

	toolkit.MustNewParameter(
		"args",
		"list of parameters for executable file",
	).SetDefaultValue([]byte("[]")),

	toolkit.MustNewParameter(
		"driver",
		"row driver with parameters that is used for interacting with cmd. The default is csv. "+
			`The structure is:`+
			`{"name": "text|csv|json", "params": { "format": "[text|bytes]"} }`,
	).SetDefaultValue([]byte(`{"name": "csv"}`)),

	toolkit.MustNewParameter(
		"validate",
		"try to encode-decode data received from cmd",
	).SetDefaultValue([]byte("false")),

	toolkit.MustNewParameter(
		"timeout",
		"timeout for sending and receiving data from cmd",
	).SetDefaultValue([]byte("2s")),

	toolkit.MustNewParameter(
		"expected_exit_code",
		"expected exit code",
	).SetDefaultValue([]byte("0")),

	toolkit.MustNewParameter(
		"skip_on_behaviour",
		"skip transformation call if one of the provided columns has null value (any) or each of the provided"+
			" column has null values (all). This option works together with skip_on_null_input parameter on columns."+
			`Possible values: "all", "any"`,
	).SetDefaultValue([]byte("all")).
		SetRawValueValidator(cmdValidateSkipBehaviour),
)

type Column struct {
	Name             string `json:"name"`
	NotAffected      bool   `json:"not_affected"`
	SkipOnNullInput  bool   `json:"skip_on_null_input"`
	SkipOriginalData bool   `json:"skip_original_data"`
}

type Cmd struct {
	*utils.CmdTransformerBase

	Columns []*Column

	name                   string
	executable             string
	args                   []string
	validateOutput         bool
	timeout                time.Duration
	expectedExitCode       int
	affectedColumns        map[int]string
	affectedColumnsIdx     []int
	transferringColumnsIdx []int
	allColumnsIdx          []int
	skipOnBehaviour        int
	checkSkip              bool
	rowDriverParams        *toolkit.RowDriverParams

	driver *toolkit.Driver
	t      *time.Ticker
	eg     *errgroup.Group
}

func NewCmd(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
) (utils.Transformer, toolkit.ValidationWarnings, error) {

	name := cmdTransformerName
	var columns []*Column
	var executable string
	var args []string
	var validate bool
	var timeout time.Duration
	var expectedExitCode int
	var skipOnBehaviourName string
	var skipOnBehaviour = skipOnAll
	var checkSkip bool
	rowDriverParams := &toolkit.RowDriverParams{}

	p := parameters["columns"]
	if _, err := p.Scan(&columns); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "columns" param: %w`, err)
	}

	p = parameters["executable"]
	if _, err := p.Scan(&executable); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "executable" param: %w`, err)
	}

	p = parameters["args"]
	if _, err := p.Scan(&args); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "args" param: %w`, err)
	}

	p = parameters["driver"]
	if _, err := p.Scan(rowDriverParams); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "mode" param: %w`, err)
	}

	p = parameters["validate"]
	if _, err := p.Scan(&validate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "validate" param: %w`, err)
	}

	p = parameters["timeout"]
	if _, err := p.Scan(&timeout); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "timeout" param: %w`, err)
	}

	p = parameters["expected_exit_code"]
	if _, err := p.Scan(&expectedExitCode); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "expected_exit_code" param: %w`, err)
	}

	p = parameters["skip_on_behaviour"]
	if _, err := p.Scan(&skipOnBehaviourName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "skip_on_behaviour" param: %w`, err)
	}
	if skipOnBehaviourName == skipOnAnyName {
		skipOnBehaviour = skipOnAny
	}

	affectedColumns := make(map[int]string)
	var warnings toolkit.ValidationWarnings
	var affectedColumnsIdx []int
	var transferringColumnsIdx []int
	var allColumnsIdx []int

	if len(columns) > 0 {
		for num, c := range columns {
			var added bool
			if c.SkipOnNullInput {
				checkSkip = true
			}
			idx, column, ok := driver.GetColumnByName(c.Name)
			if !ok {

				warnings = append(warnings, toolkit.NewValidationWarning().
					AddMeta("ElementNum", num).
					AddMeta("ColumnName", c.Name).
					SetSeverity(toolkit.ErrorValidationSeverity).
					SetMsg("column not found"))
				continue
			}
			allColumnsIdx = append(allColumnsIdx, idx)

			if !c.NotAffected {
				added = true
				affectedColumns[idx] = c.Name
				affectedColumnsIdx = append(affectedColumnsIdx, idx)
				warns := utils.ValidateSchema(driver.Table, column, nil)
				warnings = append(warnings, warns...)

			}
			if !c.SkipOriginalData {
				added = true
				transferringColumnsIdx = append(transferringColumnsIdx, idx)
			}

			if !added {
				warnings = append(warnings, toolkit.NewValidationWarning().
					AddMeta("ElementNum", num).
					AddMeta("ColumnName", c.Name).
					SetSeverity(toolkit.ErrorValidationSeverity).
					SetMsg("column not added either into transferred list or affected list"))
				continue
			}
		}
	} else {
		for idx := range driver.Table.Columns {
			transferringColumnsIdx = append(transferringColumnsIdx, idx)
			affectedColumnsIdx = append(affectedColumnsIdx, idx)
		}
	}

	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	api, err := toolkit.NewApi(rowDriverParams, transferringColumnsIdx, affectedColumnsIdx, driver)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating InteractionApi: %w", err)
	}

	cct := utils.NewCmdTransformerBase(name, expectedExitCode, driver, api)

	ct := &Cmd{
		CmdTransformerBase:     cct,
		driver:                 driver,
		name:                   name,
		Columns:                columns,
		executable:             executable,
		args:                   args,
		rowDriverParams:        rowDriverParams,
		validateOutput:         validate,
		timeout:                timeout,
		expectedExitCode:       expectedExitCode,
		affectedColumns:        affectedColumns,
		affectedColumnsIdx:     affectedColumnsIdx,
		transferringColumnsIdx: transferringColumnsIdx,
		allColumnsIdx:          allColumnsIdx,
		skipOnBehaviour:        skipOnBehaviour,
		checkSkip:              checkSkip,
		t:                      time.NewTicker(timeout),
	}

	return ct, warnings, nil
}

func (c *Cmd) GetAffectedColumns() map[int]string {
	return c.affectedColumns
}

func (c *Cmd) watchForTimeout(ctx context.Context) error {
	for {
		if c.ProcessedLines > -1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.DoneCh:
			return nil
		default:
		}
		time.Sleep(1 * time.Second)
	}
	c.t.Reset(c.timeout)
	for {
		lastValue := c.ProcessedLines
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.DoneCh:
			return nil
		case <-c.t.C:
			if lastValue == c.ProcessedLines {
				if err := c.Cancel(); err != nil {
					log.Warn().Err(err).Msg("error terminating transformer")
				}
				return ErrRowTransformationTimeout
			}
		}
	}
}

func (c *Cmd) Init(ctx context.Context) error {
	if err := c.BaseInit(c.executable, c.args); err != nil {
		return err
	}
	c.eg = &errgroup.Group{}
	c.eg.Go(func() error {
		return c.stderrForwarder(ctx)
	})

	c.eg.Go(func() error {
		return c.watchForTimeout(ctx)
	})

	c.eg.Go(func() error {
		if err := c.Cmd.Wait(); err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				if exitErr.ExitCode() != c.expectedExitCode {
					log.Warn().
						Str("TableSchema", c.driver.Table.Schema).
						Str("TableName", c.driver.Table.Name).
						Str("TransformerName", c.name).
						Int("TransformerExitCode", c.Cmd.ProcessState.ExitCode()).
						Msg("unexpected exit code")
					return fmt.Errorf("unexpeted command exit code: exepected %d received %d",
						c.expectedExitCode, c.Cmd.ProcessState.ExitCode())
				}
				return nil
			} else {
				log.Error().
					Err(err).
					Str("TableSchema", c.driver.Table.Schema).
					Str("TableName", c.driver.Table.Name).
					Str("TransformerName", c.name).
					Int("TransformerPid", c.Cmd.Process.Pid).
					Msg("cmd transformer exited with error")
				return fmt.Errorf("transformer exited with error: %w", err)
			}
		}

		log.Debug().
			Str("TableSchema", c.driver.Table.Schema).
			Str("TableName", c.driver.Table.Name).
			Str("TransformerName", c.name).
			Int("TransformerPid", c.Cmd.Process.Pid).
			Msg("transformer exited normally")
		return nil
	})

	return nil
}

func (c *Cmd) Done(ctx context.Context) error {
	if err := c.BaseDone(); err != nil {
		return err
	}
	if err := c.eg.Wait(); err != nil {
		log.Warn().
			Err(err).
			Str("TableSchema", c.driver.Table.Schema).
			Str("TableName", c.driver.Table.Name).
			Str("TransformerName", c.name).
			Msg("cmd transformer goroutine exited with error")
		return fmt.Errorf("cmd transformer goroutine exited with error: %w", err)
	}
	return nil
}

func (c *Cmd) stderrForwarder(ctx context.Context) error {
	for {
		line, _, err := c.StderrReader.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
				return nil
			}
			log.Debug().Err(err).Msg("line reader error")
			return err
		}

		log.Warn().
			Str("TableSchema", c.driver.Table.Schema).
			Str("TableName", c.driver.Table.Name).
			Str("TransformerName", c.name).
			Int("TransformerPid", c.Cmd.Process.Pid).
			Msg("stderr forwarding")
		fmt.Printf("\tDATA: %s\n", string(line))

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (c *Cmd) needSkip(r *toolkit.Record) (bool, error) {
	var score int
	for _, idx := range c.allColumnsIdx {
		v, err := r.GetRawAttributeValueByIdx(idx)
		if err != nil {
			return false, fmt.Errorf("error getting raw attribute value: %w", err)
		}
		if v.IsNull {
			score++
		}
	}
	if score == len(c.allColumnsIdx) && skipOnAll == c.skipOnBehaviour {
		return true, nil
	} else if score > 0 && skipOnAny == c.skipOnBehaviour {
		return true, nil
	}
	return false, nil
}

func (c *Cmd) validate(r *toolkit.Record) error {
	for _, idx := range c.affectedColumnsIdx {
		_, err := r.GetAttributeValueByIdx(idx)
		if err != nil {
			return fmt.Errorf("error validating received attribute \"%s\" value from cmd: %w", r.Driver.Table.Columns[idx].Name, err)
		}
	}
	return nil
}

func (c *Cmd) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var err error
	if c.checkSkip {
		var skip bool
		skip, err = c.needSkip(r)
		if err != nil {
			return nil, err
		}
		if skip {
			c.CmdTransformerBase.ProcessedLines++
			return r, nil
		}
	}

	r, err = c.CmdTransformerBase.Transform(ctx, r)
	if err != nil {
		return nil, err
	}
	if c.validateOutput {
		if err = c.validate(r); err != nil {
			return nil, fmt.Errorf("tuple validation error: %w", err)
		}
	}
	return r, nil
}

func cmdValidateFormat(p *toolkit.Parameter, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
	value := string(v)
	if value != cmdRowDriverCsvName && value != cmdRowDriverTextName &&
		value != cmdRowDriverJsonName {
		return toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				AddMeta("ParameterName", p.Name).
				AddMeta("ParameterValue", value).
				SetSeverity(toolkit.ErrorValidationSeverity).
				SetMsg("unsupported format type: must be one of csv, json, text"),
		}, nil
	}
	return nil, nil
}

func cmdValidateSkipBehaviour(p *toolkit.Parameter, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
	value := string(v)
	if value != skipOnAnyName && value != skipOnAllName {
		return toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				AddMeta("ParameterName", p.Name).
				AddMeta("ParameterValue", value).
				SetMsg(`unsupported skip_on type: must be one of "all" or "any"`),
		}, nil
	}
	return nil, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(CmdTransformerDefinition)
}
