package transformers

import (
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	cmdInteractionFormatTextName = "text"
	cmdInteractionFormatJsonName = "json"
	cmdInteractionFormatCsvName  = "csv"
)

var CmdTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"Cmd",
		"Transform data via external program using stdin and stdout",
	),

	nil,

	toolkit.MustNewParameter(
		"columns",
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
		"format",
		"interaction format [csv, json, text]",
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
		"transformation_timeout",
		"timeout for sending and receiving data from cmd",
	).SetDefaultValue([]byte("2s")),

	toolkit.MustNewParameter(
		"expected_exit_code",
		"expected exit code",
	).SetDefaultValue([]byte("0")),
)

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
