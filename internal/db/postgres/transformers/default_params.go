package transformers

import (
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	randomEngineName = "random"
	hashEngineName   = "hash"
)

var (
	engineParameterDefinition = toolkit.MustNewParameterDefinition(
		"engine",
		"The engine used for generating the values [random, hash]",
	).SetDefaultValue([]byte("random")).
		SetRawValueValidator(engineValidator)

	keepNullParameterDefinition = toolkit.MustNewParameterDefinition(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(toolkit.ParamsValue("true"))
)

func engineValidator(p *toolkit.ParameterDefinition, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
	value := string(v)
	if value != randomEngineName && value != hashEngineName {
		return toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetMsg("Invalid engine value").
				AddMeta("ParameterValue", value).
				SetSeverity(toolkit.ErrorValidationSeverity),
		}, nil
	}
	return nil, nil
}
