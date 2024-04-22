package transformers

import (
	"fmt"
	"strings"

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

	minRatioParameterDefinition = toolkit.MustNewParameterDefinition(
		"min_ratio",
		"min random percentage for noise",
	).SetDefaultValue(toolkit.ParamsValue("0.05"))

	maxRatioParameterDefinition = toolkit.MustNewParameterDefinition(
		"max_ratio",
		"max random percentage for noise",
	).SetDefaultValue(toolkit.ParamsValue("0.2"))

	truncateDateParameterDefinition = toolkit.MustNewParameterDefinition(
		"truncate",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
	).SetRawValueValidator(validateDateTruncationParameterValue)
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
