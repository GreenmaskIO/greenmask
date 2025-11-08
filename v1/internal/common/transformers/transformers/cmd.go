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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/transformers/cmd"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const (
	skipOnAny = iota
	skipOnAll
)

const (
	skipOnAnyName = "any"
	skipOnAllName = "all"
)

var (
	CmdTransformerName = "Cmd"

	ErrUnexpectedCmdExitCode = errors.New("unexpected cmd transformer exit code")
)

type cmdColumnContainer struct {
	Name             string `json:"name"`
	NotAffected      bool   `json:"not_affected"`
	SkipOnNullInput  bool   `json:"skip_on_null_input"`
	SkipOriginalData bool   `json:"skip_original_data"`
	Position         int    `json:"position"`
}

func (m *cmdColumnContainer) IsAffected() bool {
	return !m.NotAffected
}

func (m *cmdColumnContainer) ColumnName() string {
	return m.Name
}

var CMDTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		CmdTransformerName,
		"Transform data via external program using stdin and stdout interaction",
	),

	NewCmd,

	commonparameters.MustNewParameterDefinition(
		"columns",
		"affected column names. If empty use the whole tuple."+
			"The structure:"+
			`{`+
			`"name": "type:string, required:true, description: column Name",`+
			`"not_affected": "type:bool, required:false, description: is column affected in transformation", `+
			`"skip_original_data":  "type:bool, required:false, description: is original data required for transformer",`+
			`"skip_on_null_input":  "type:bool, required:false, description: skip transformation on null input"`+
			`}`,
	).SetDefaultValue([]byte("[]")).
		SetColumnContainer(commonparameters.NewColumnContainerProperties().
			SetUnmarshaler(defaultColumnContainerUnmarshaler[*cmdColumnContainer]),
		),

	commonparameters.MustNewParameterDefinition(
		"executable",
		"path to executable file",
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"args",
		"list of parameters for executable file",
	).SetDefaultValue([]byte("[]")),

	commonparameters.MustNewParameterDefinition(
		"driver",
		"row driver settings required for interaction with cmd. Supported drivers: json, csv, text.\n"+
			"The json driver has additional configuration options. See documentation for details.\n"+
			"Default is json driver with text data format and columns by indexes.",
	).SetDefaultValue(utils.Must(json.Marshal(cmd.DefaultRowDriverParams))).
		SetRawValueValidator(func(ctx context.Context, p *commonparameters.ParameterDefinition, v commonmodels.ParamsValue) error {
			var res cmd.RowDriverSetting
			if err := json.Unmarshal(v, &res); err != nil {
				validationcollector.FromContext(ctx).
					Add(commonmodels.NewValidationWarning().
						SetSeverity(commonmodels.ValidationSeverityError).
						AddMeta(commonmodels.MetaKeyParameterName, p.Name).
						AddMeta(commonmodels.MetaKeyParameterValue, string(v)).
						SetMsg(fmt.Sprintf("unable to unmarshal driver params: %v", err)))
				return commonmodels.ErrFatalValidationError
			}
			if err := res.Validate(); err != nil {
				validationcollector.FromContext(ctx).
					Add(commonmodels.NewValidationWarning().
						SetSeverity(commonmodels.ValidationSeverityError).
						AddMeta(commonmodels.MetaKeyParameterName, p.Name).
						AddMeta(commonmodels.MetaKeyParameterValue, string(v)).
						SetMsg(fmt.Sprintf("invalid driver params: %v", err)))
				return commonmodels.ErrFatalValidationError
			}
			return nil
		}),

	commonparameters.MustNewParameterDefinition(
		"validate",
		"try to encode-decode data received from cmd",
	).SetDefaultValue([]byte("false")),

	commonparameters.MustNewParameterDefinition(
		"timeout",
		"timeout for sending and receiving data from cmd",
	).SetDefaultValue([]byte("2s")),

	commonparameters.MustNewParameterDefinition(
		"expected_exit_code",
		"expected exit code",
	).SetDefaultValue([]byte("0")),

	commonparameters.MustNewParameterDefinition(
		"skip_on_behaviour",
		"skip transformation call if one of the provided columns has null value (any) or each of the provided"+
			" column has null values (all). This option works together with skip_on_null_input parameter on columns."+
			`Possible values: "all", "any"`,
	).SetDefaultValue([]byte("all")).
		SetRawValueValidator(cmdValidateSkipBehaviour),
)

type Cmd struct {
	*cmd.TransformerBase

	Columns []*cmdColumnContainer

	executable       string
	args             []string
	validateOutput   bool
	timeout          time.Duration
	expectedExitCode int
	affectedColumns  map[int]string
	skipOnBehaviour  int
	rowDriverParams  *cmd.RowDriverSetting
	config           *cmdConfig
	table            *commonmodels.Table
	eg               *errgroup.Group
}

func NewCmd(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	var skipOnBehaviour = skipOnAll
	columns, affectedColumns, err := getColumnContainerParameter[*cmdColumnContainer](
		ctx, tableDriver, parameters, "columns",
	)
	if err != nil {
		return nil, fmt.Errorf("get \"columns\" parameter: %w", err)
	}

	executable, err := getParameterValueWithName[string](ctx, parameters, "executable")
	if err != nil {
		return nil, fmt.Errorf("get \"executable\" param: %w", err)
	}

	args, err := getParameterValueWithName[[]string](ctx, parameters, "args")
	if err != nil {
		return nil, fmt.Errorf("get \"args\" param: %w", err)
	}

	rowDriverParams, err := getParameterValueWithNameAndDefault[cmd.RowDriverSetting](
		ctx, parameters, "driver", cmd.DefaultRowDriverParams,
	)
	if err != nil {
		return nil, fmt.Errorf("get \"driver\" param: %w", err)
	}

	validate, err := getParameterValueWithName[bool](ctx, parameters, "validate")
	if err != nil {
		return nil, fmt.Errorf("get \"validate\" param: %w", err)
	}

	timeout, err := getParameterValueWithName[time.Duration](ctx, parameters, "timeout")
	if err != nil {
		return nil, fmt.Errorf("get \"timeout\" param: %w", err)
	}

	expectedExitCode, err := getParameterValueWithName[int](ctx, parameters, "expected_exit_code")
	if err != nil {
		return nil, fmt.Errorf("get \"expected_exit_code\" param: %w", err)
	}

	skipOnBehaviourName, err := getParameterValueWithName[string](ctx, parameters, "skip_on_behaviour")
	if err != nil {
		return nil, fmt.Errorf("get \"skip_on_behaviour\" param: %w", err)
	}

	if skipOnBehaviourName == skipOnAnyName {
		skipOnBehaviour = skipOnAny
	}

	config, err := validateCMDColumnsAndSetDefault(ctx, tableDriver, columns)
	if err != nil {
		return nil, fmt.Errorf("validate columns parameter: %w", err)
	}

	proto, err := cmd.NewProto(
		&rowDriverParams,
		config.TransferringColumns,
		config.AffectedColumns,
	)
	if err != nil {
		return nil, fmt.Errorf("create interaction API: %w", err)
	}

	cct := cmd.NewTransformerBase(
		CmdTransformerName, expectedExitCode, timeout, tableDriver.Table(), proto,
	)

	ct := &Cmd{
		TransformerBase:  cct,
		table:            tableDriver.Table(),
		Columns:          columns,
		executable:       executable,
		args:             args,
		rowDriverParams:  &rowDriverParams,
		validateOutput:   validate,
		timeout:          timeout,
		expectedExitCode: expectedExitCode,
		affectedColumns:  affectedColumns,
		skipOnBehaviour:  skipOnBehaviour,
		config:           &config,
		eg:               &errgroup.Group{},
	}

	return ct, nil
}

func (c *Cmd) GetAffectedColumns() map[int]string {
	return c.affectedColumns
}

func (c *Cmd) Init(ctx context.Context) error {
	logger := log.Ctx(ctx).With().
		Str("TableSchema", c.table.Schema).
		Str("TableName", c.table.Name).
		Str("TransformerName", CmdTransformerName).
		Logger()

	if err := c.TransformerBase.Init(ctx, c.executable, c.args); err != nil {
		return err
	}
	c.eg.Go(func() error {
		return c.stderrForwarder(ctx)
	})

	c.eg.Go(func() error {
		if err := c.Cmd.Wait(); err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				if exitErr.ExitCode() != c.expectedExitCode {
					logger.Warn().
						Int("TransformerExitCode", c.Cmd.ProcessState.ExitCode()).
						Err(ErrUnexpectedCmdExitCode).
						Msg("unexpected exit code")
					return fmt.Errorf("exepected exit code %d received %d: %w",
						c.expectedExitCode, c.Cmd.ProcessState.ExitCode(), ErrUnexpectedCmdExitCode,
					)
				}
				return nil
			} else {
				logger.Error().
					Err(err).
					Int("TransformerPid", c.Cmd.Process.Pid).
					Msg("cmd transformer exited with error")
				return fmt.Errorf("transformer exited with error: %w", err)
			}
		}

		logger.Debug().
			Int("TransformerPid", c.Cmd.Process.Pid).
			Msg("transformer exited normally")
		return nil
	})

	return nil
}

func (c *Cmd) Done(ctx context.Context) error {
	logger := log.Ctx(ctx).With().
		Str("TableSchema", c.table.Schema).
		Str("TableName", c.table.Name).
		Str("TransformerName", CmdTransformerName).
		Logger()

	if err := c.TransformerBase.Done(ctx); err != nil {
		return fmt.Errorf("transformer done with error: %w", err)
	}
	if err := c.eg.Wait(); err != nil {
		logger.Warn().
			Err(err).
			Msg("cmd transformer goroutine exited with error")
		return err
	}
	return nil
}

func (c *Cmd) stderrForwarder(ctx context.Context) error {
	logger := log.Ctx(ctx).With().
		Str("TableSchema", c.table.Schema).
		Str("TableName", c.table.Name).
		Str("TransformerName", CmdTransformerName).
		Int("TransformerPid", c.Cmd.Process.Pid).
		Logger()
	for {
		line, err := c.ReceiveStderrLine(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
				return nil
			}
			log.Debug().Err(err).Msg("line reader error")
			return err
		}

		logger.Warn().
			Msg("stderr forwarding")
		// TODO: Consider logging instead of printing
		// 			The problem is that the line can be too long and it's hard to read in logs.
		fmt.Printf("\tDATA: %s\n", string(line))

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (c *Cmd) needSkip(r commonininterfaces.Recorder) (bool, error) {
	var score int
	for _, c := range c.config.TransferringColumns {
		v, err := r.GetRawColumnValueByIdx(c.Column.Idx)
		if err != nil {
			return false, fmt.Errorf("get value: %w", err)
		}
		if v.IsNull {
			score++
		}
	}
	if score == len(c.config.TransferringColumns) && skipOnAll == c.skipOnBehaviour {
		return true, nil
	} else if score > 0 && skipOnAny == c.skipOnBehaviour {
		return true, nil
	}
	return false, nil
}

func (c *Cmd) validate(r commonininterfaces.Recorder) error {
	for _, col := range c.config.AffectedColumns {
		_, err := r.GetColumnValueByIdx(col.Column.Idx)
		if err != nil {
			return errors.Join(commonmodels.ErrValueValidationFailed, fmt.Errorf(
				"validate received column \"%s\" value: %w",
				r.TableDriver().Table().Columns[col.Column.Idx].Name, err,
			))
		}
	}
	return nil
}

func (c *Cmd) Transform(ctx context.Context, r commonininterfaces.Recorder) error {
	var err error
	if c.config.CheckCanSkip {
		var skip bool
		skip, err = c.needSkip(r)
		if err != nil {
			return fmt.Errorf("check need skip: %w", err)
		}
		if skip {
			c.TransformerBase.ProcessedLines++
			return nil
		}
	}

	err = c.TransformerBase.Transform(ctx, r)
	if err != nil {
		return fmt.Errorf("transform: %w", err)
	}
	if c.validateOutput {
		if err = c.validate(r); err != nil {
			return fmt.Errorf("validate transformed data: %w", err)
		}
	}
	return nil
}

func cmdValidateSkipBehaviour(ctx context.Context, _ *commonparameters.ParameterDefinition, v models.ParamsValue) error {
	switch string(v) {
	case skipOnAnyName, skipOnAllName:
		return nil
	default:
		validationcollector.FromContext(ctx).
			Add(commonmodels.NewValidationWarning().
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("ParameterValue", string(v)).
				SetMsg(`unsupported skip_on type: must be one of "all" or "any"`))
		return commonmodels.ErrFatalValidationError
	}
}

type cmdConfig struct {
	CheckCanSkip        bool
	TransferringColumns []*cmd.ColumnMapping
	AffectedColumns     []*cmd.ColumnMapping
}

func validateCMDColumnsAndSetDefault(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	columns []*cmdColumnContainer,
) (cmdConfig, error) {
	var (
		transferringColumns []*cmd.ColumnMapping
		affectedColumns     []*cmd.ColumnMapping
	)
	if len(columns) == 0 {
		allColumns := tableDriver.Table().Columns
		for i := range allColumns {
			transferringColumns = append(transferringColumns, &cmd.ColumnMapping{
				Column:   &allColumns[i],
				Position: i,
			})
			affectedColumns = append(affectedColumns, &cmd.ColumnMapping{
				Column:   &allColumns[i],
				Position: i,
			})
		}
		return cmdConfig{
			TransferringColumns: transferringColumns,
			AffectedColumns:     affectedColumns,
			CheckCanSkip:        false,
		}, nil
	}
	var checkSkip bool
	for i, c := range columns {
		var added bool
		if c.SkipOnNullInput {
			checkSkip = true
		}
		column, err := tableDriver.GetColumnByName(c.Name)
		if err != nil {
			validationcollector.FromContext(ctx).Add(commonmodels.NewValidationWarning().
				AddMeta("ElementNum", i).
				AddMeta("ColumnName", c.Name).
				SetSeverity(commonmodels.ValidationSeverityError).
				SetMsg("column not found"))
			return cmdConfig{}, commonmodels.ErrFatalValidationError
		}

		if !columns[i].NotAffected {
			added = true
			affectedColumns = append(affectedColumns, &cmd.ColumnMapping{
				Column:   column,
				Position: c.Position,
			})
		}
		if !columns[i].SkipOriginalData {
			added = true
			transferringColumns = append(transferringColumns, &cmd.ColumnMapping{
				Column:   column,
				Position: c.Position,
			})
		}

		if !added {
			validationcollector.FromContext(ctx).Add(commonmodels.NewValidationWarning().
				AddMeta("ElementNum", i).
				AddMeta("ColumnName", c.Name).
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("Hint", "ensure that either not_affected is false or skip_original_data is false").
				SetMsg("column not added either into transferred list or affected list"))
			return cmdConfig{}, commonmodels.ErrFatalValidationError
		}
	}
	return cmdConfig{
		TransferringColumns: transferringColumns,
		AffectedColumns:     affectedColumns,
		CheckCanSkip:        checkSkip,
	}, nil
}
