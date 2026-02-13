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
	"fmt"

	"github.com/greenmaskio/greenmask/v1/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/pkg/common/models"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/generators/transformers"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/utils"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

const TransformerNameNoiseNumeric = "NoiseNumeric"

var NoiseNumericTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TransformerNameNoiseNumeric,
		"Add noise to numeric value in min and max thresholds",
	).AddMeta(utils.AllowApplyForReferenced, true).
		AddMeta(utils.RequireHashEngineParameter, true),

	NewNumericFloatTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(parameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypeClasses(models.TypeClassNumeric).
		SetSkipOnNull(true),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"decimal",
		"Numbers of decimal",
	).SetSupportTemplate(true).
		SetDefaultValue(models.ParamsValue("4")),

	parameters.MustNewParameterDefinition(
		"min",
		"min float value threshold",
	).SetSupportTemplate(true).
		LinkParameter("column").
		SetDynamicMode(
			parameters.NewDynamicModeProperties().
				SetColumnProperties(
					parameters.NewColumnProperties().
						SetAllowedColumnTypeClasses(
							models.TypeClassFloat,
							models.TypeClassInt,
							models.TypeClassNumeric,
						),
				).SetUnmarshaler(numericTypeUnmarshaler),
		),

	parameters.MustNewParameterDefinition(
		"max",
		"max float value threshold",
	).SetSupportTemplate(true).
		LinkParameter("column").
		SetDynamicMode(
			parameters.NewDynamicModeProperties().
				SetColumnProperties(
					parameters.NewColumnProperties().
						SetAllowedColumnTypeClasses(
							models.TypeClassFloat,
							models.TypeClassInt,
							models.TypeClassNumeric,
						),
				).SetUnmarshaler(numericTypeUnmarshaler),
		),

	parameters.MustNewParameterDefinition(
		"type_size",
		"size of the numeric type (total number of digits)",
	).SetDefaultValue(models.ParamsValue("4")),

	defaultMinRatioParameterDefinition,

	defaultMaxRatioParameterDefinition,

	defaultEngineParameterDefinition,
)

// TODO: Add numeric introspection (getting the Numering settings)
type NoiseNumericTransformer struct {
	t               *transformers.NoiseNumericTransformer
	columnName      string
	columnIdx       int
	decimal         int32
	affectedColumns map[int]string
	dynamicMode     bool

	minAllowedValue decimal.Decimal
	maxAllowedValue decimal.Decimal
	numericSize     int

	maxParam parameters.Parameterizer
	minParam parameters.Parameterizer

	transform func(decimal.Decimal) (decimal.Decimal, error)
}

func NewNumericFloatTransformer(
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
) (interfaces.Transformer, error) {
	var minValueThreshold, maxValueThreshold *decimal.Decimal

	minParam := parameters["min"]
	maxParam := parameters["max"]

	dynamicMode := isInDynamicMode(parameters)

	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	engine, err := getParameterValueWithName[string](ctx, parameters, ParameterNameEngine)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	if !dynamicMode {
		minValueThreshold, maxValueThreshold, err = getOptionalMinAndMaxThresholds[decimal.Decimal](minParam, maxParam)
		if err != nil {
			return nil, fmt.Errorf("get min and max thresholds: %w", err)
		}
	}

	limiter, err := validateNoiseNumericTypeAndSetLimit(
		ctx,
		bigIntegerTransformerGenByteLength,
		minValueThreshold,
		maxValueThreshold,
	)
	if err != nil {
		return nil, err
	}

	decimalVal, err := getParameterValueWithName[int32](ctx, parameters, "decimal")
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	minRatio, err := getParameterValueWithName[float64](ctx, parameters, "min_ratio")
	if err != nil {
		return nil, fmt.Errorf("get \"max_ratio\" param: %w", err)
	}
	maxRatio, err := getParameterValueWithName[float64](ctx, parameters, "max_ratio")
	if err != nil {
		return nil, fmt.Errorf("get \"max_ratio\" param: %w", err)
	}

	typeSize := column.Size
	if typeSize == 0 {
		log.Ctx(ctx).
			Info().
			Msg("unable to detect float size from column length, trying to get it from \"type_size\" parameter")
		typeSize, err = getParameterValueWithName[int](
			ctx,
			parameters,
			"type_size",
		)
		if err != nil {
			return nil, fmt.Errorf("scan \"type_size\" param: %w", err)
		}
		log.Ctx(ctx).
			Info().
			Msgf("using float size %d from \"type_size\" parameter", typeSize)
	}

	limiter.SetPrecision(decimalVal)

	t := transformers.NewNoiseNumericTransformer(limiter, minRatio, maxRatio)

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("set generator: %w", err)
	}

	return &NoiseNumericTransformer{
		t:          t,
		columnName: columnName,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx: column.Idx,
		decimal:   decimalVal,

		minParam: minParam,
		maxParam: maxParam,

		minAllowedValue: limiter.MinValue,
		maxAllowedValue: limiter.MaxValue,
		numericSize:     typeSize,
		dynamicMode:     dynamicMode,

		transform: t.Transform,
	}, nil
}

func (t *NoiseNumericTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *NoiseNumericTransformer) Init(context.Context) error {
	if t.dynamicMode {
		t.transform = t.dynamicTransform
	}
	return nil
}

func (t *NoiseNumericTransformer) Done(context.Context) error {
	return nil
}

func (t *NoiseNumericTransformer) dynamicTransform(original decimal.Decimal) (decimal.Decimal, error) {
	var minVal, maxVal decimal.Decimal
	err := t.minParam.Scan(&minVal)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	err = t.maxParam.Scan(&maxVal)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	limiter, err := getNoiseNumericLimiterForDynamicParameter(t.numericSize, minVal, maxVal, t.minAllowedValue, t.maxAllowedValue)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	limiter.SetPrecision(t.decimal)
	return t.t.SetDynamicLimiter(limiter).Transform(original)
}

func (t *NoiseNumericTransformer) Transform(_ context.Context, r interfaces.Recorder) error {
	var val decimal.Decimal
	isNull, err := r.ScanColumnValueByIdx(t.columnIdx, &val)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if isNull {
		return nil
	}

	res, err := t.transform(val)
	if err != nil {
		return fmt.Errorf("transform value: %w", err)
	}

	if err = r.SetColumnValueByIdx(t.columnIdx, res); err != nil {
		return fmt.Errorf("set new value: %w", err)
	}
	return nil
}

func (t *NoiseNumericTransformer) Describe() string {
	return TransformerNameNoiseNumeric
}

func validateNoiseNumericTypeAndSetLimit(
	ctx context.Context, size int, requestedMinValue, requestedMaxValue *decimal.Decimal,
) (*transformers.NoiseNumericLimiter, error) {
	minVal, maxVal, err := getNumericThresholds(ctx, size, requestedMinValue, requestedMaxValue)
	if err != nil {
		return nil, err
	}

	limiter, err := transformers.NewNoiseNumericLimiter(minVal, maxVal)
	if err != nil {
		return nil, fmt.Errorf("create limiter by size: %w", err)
	}

	return limiter, nil
}

func getNoiseNumericLimiterForDynamicParameter(
	numericSize int, requestedMinValue, requestedMaxValue,
	minAllowedValue, maxAllowedValue decimal.Decimal,
) (*transformers.NoiseNumericLimiter, error) {

	if !numericLimitIsValid(requestedMinValue, minAllowedValue, maxAllowedValue) {
		return nil, fmt.Errorf("requested dynamic parameter min value is out of range of NUMERIC(%d) size", numericSize)
	}

	if !numericLimitIsValid(requestedMaxValue, minAllowedValue, maxAllowedValue) {
		return nil, fmt.Errorf("requested dynamic parameter max value is out of range of NUMERIC(%d) size", numericSize)
	}

	limiter, err := transformers.NewNoiseNumericLimiter(minAllowedValue, maxAllowedValue)
	if err != nil {
		return nil, err
	}

	if !requestedMinValue.Equal(decimal.NewFromInt(0)) || !requestedMaxValue.Equal(decimal.NewFromInt(0)) {
		limiter, err = transformers.NewNoiseNumericLimiter(requestedMinValue, requestedMaxValue)
		if err != nil {
			return nil, err
		}
	}
	return limiter, nil
}
