package transformers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/rs/zerolog/log"
)

const (
	cmdInteractionFormatTextName = "text"
	cmdInteractionFormatJsonName = "json"
	cmdInteractionFormatCsvName  = "csv"
)

var cmdTransformerName = "Cmd"

var CmdTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		cmdTransformerName,
		"Transform data via external program using stdin and stdout",
	),

	NewCmd,

	toolkit.MustNewParameter(
		"Columns",
		"affected column names. If empty use the whole tuple",
	).SetDefaultValue([]byte("[]")),

	toolkit.MustNewParameter(
		"executable",
		"path to executable file",
	).SetRequired(true),

	toolkit.MustNewParameter(
		"args",
		"list of parameters for executable file",
	),

	toolkit.MustNewParameter(
		"mode",
		"interaction mode [csv, json, text]",
	).SetDefaultValue([]byte("csv")).
		SetRawValueValidator(cmdValidateFormat),

	toolkit.MustNewParameter(
		"keep_null",
		"do not transform NULL values",
	).SetDefaultValue([]byte("true")),

	toolkit.MustNewParameter(
		"validate_output",
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
)

type Cmd struct {
	*utils.CmdTransformerBase

	Columns []*Column

	name             string
	executable       string
	args             []string
	mode             string
	validateOutput   bool
	timeout          time.Duration
	expectedExitCode int
	affectedColumns  map[int]string

	driver *toolkit.Driver
}

type Column struct {
	Name             string `json:"name"`
	NotAffected      bool   `json:"not_affected"`
	SkipOnNullInput  bool   `json:"skip_on_null_input"`
	SkipOriginalData bool   `json:"skip_original_data"`
}

func NewCmd(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
) (utils.Transformer, toolkit.ValidationWarnings, error) {

	name := cmdTransformerName
	var columns []*Column
	var executable string
	var args []string
	var mode = toolkit.CsvModeName
	var validateOutput bool
	var timeout time.Duration
	var expectedExitCode int

	affectedColumns := make(map[int]string)
	var affectedColumnsIdx []int
	var transferringColumnsIdx []int

	if len(columns) > 0 {
		for num, c := range columns {
			var added bool
			idx, _, ok := driver.GetColumnByName(c.Name)
			if !ok {
				return nil, toolkit.ValidationWarnings{
					toolkit.NewValidationWarning().
						AddMeta("ElementNum", num).
						AddMeta("ColumnName", c.Name).
						SetSeverity(toolkit.ErrorValidationSeverity).
						SetMsg("column not found"),
				}, nil
			}

			if c.NotAffected {
				added = true
				affectedColumns[idx] = c.Name
				affectedColumnsIdx = append(affectedColumnsIdx, idx)
			}
			if !c.SkipOriginalData {
				added = true
				transferringColumnsIdx = append(transferringColumnsIdx, idx)
			}

			if !added {
				return nil, toolkit.ValidationWarnings{
					toolkit.NewValidationWarning().
						AddMeta("ElementNum", num).
						AddMeta("ColumnName", c.Name).
						SetSeverity(toolkit.ErrorValidationSeverity).
						SetMsg("column not added either into transferred list or affected list"),
				}, nil
			}
		}
	} else {
		for idx, _ := range driver.Table.Columns {
			transferringColumnsIdx = append(transferringColumnsIdx, idx)
			affectedColumnsIdx = append(affectedColumnsIdx, idx)
		}
	}

	api, err := utils.NewApi(mode, transferringColumnsIdx, affectedColumnsIdx)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating InteractionApi: %w", err)
	}

	cct := utils.NewCmdTransformerBase(name, expectedExitCode, driver, api)

	ct := &Cmd{
		CmdTransformerBase: cct,
		driver:             driver,
		name:               name,
		Columns:            columns,
		executable:         executable,
		args:               args,
		mode:               mode,
		validateOutput:     validateOutput,
		timeout:            timeout,
		expectedExitCode:   expectedExitCode,
		affectedColumns:    affectedColumns,
	}

	return ct, nil, nil
}

func (c *Cmd) GetAffectedColumns() map[int]string {
	return nil
}

func (c *Cmd) Init(ctx context.Context) (err error) {
	return nil
}

func (c *Cmd) Done(ctx context.Context) error {
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

func cmdValidateFormat(p *toolkit.Parameter, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
	value := string(v)
	if value != cmdInteractionFormatCsvName && value != cmdInteractionFormatTextName &&
		value != cmdInteractionFormatJsonName {
		return toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				AddMeta("ParameterName", p.Name).
				AddMeta("ParameterValue", value).
				SetMsg("unsupported format type: must be one of csv, json, text"),
		}, nil
	}
	return nil, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(CmdTransformerDefinition)
}
