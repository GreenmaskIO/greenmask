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

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/rs/zerolog/log"
)

const (
	TransformerNameNoiseFloat = "NoiseFloat"

	float4Length = 4
	float8Length = 8
)

var NoiseFloatTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TransformerNameNoiseFloat,
		"Add noise to float value in min and max thresholds",
	).AddMeta(utils.AllowApplyForReferenced, true).
		AddMeta(utils.RequireHashEngineParameter, true),

	NewNoiseFloatTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(models.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypeClasses(models.TypeClassFloat).
		SetSkipOnNull(true),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"decimal",
		"Number of decimal places to use",
	).SetSupportTemplate(true).
		SetDefaultValue(models.ParamsValue("4")),

	parameters.MustNewParameterDefinition(
		"min",
		"min float value threshold",
	).LinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			parameters.NewDynamicModeProperties().
				SetColumnProperties(
					models.NewColumnProperties().
						SetAllowedColumnTypeClasses(
							models.TypeClassInt,
							models.TypeClassFloat,
						),
				),
		),

	parameters.MustNewParameterDefinition(
		"max",
		"max float value threshold",
	).LinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			parameters.NewDynamicModeProperties().
				SetColumnProperties(
					models.NewColumnProperties().
						SetAllowedColumnTypeClasses(
							models.TypeClassInt,
							models.TypeClassFloat,
						),
				),
		),

	defaultFloatTypeSizeParameterDefinition,

	defaultMinRatioParameterDefinition,

	defaultMaxRatioParameterDefinition,

	defaultEngineParameterDefinition,
)

type NoiseFloatTransformer struct {
	t               *transformers.NoiseFloat64Transformer
	columnName      string
	columnIdx       int
	decimal         int
	affectedColumns map[int]string
	dynamicMode     bool
	floatSize       int

	maxParam parameters.Parameterizer
	minParam parameters.Parameterizer

	transform func(float64) (float64, error)
}

func NewNoiseFloatTransformer(
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
) (interfaces.Transformer, error) {
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
			return nil, fmt.Errorf("unable to scan \"type_size\" param: %w", err)
		}
		log.Ctx(ctx).
			Info().
			Msgf("using float size %d from \"type_size\" parameter", typeSize)
	}

	var minValueThreshold, maxValueThreshold *float64
	if !dynamicMode {
		minValueThreshold, maxValueThreshold, err = getOptionalMinAndMaxThresholds[float64](minParam, maxParam)
		if err != nil {
			return nil, fmt.Errorf("get min and max thresholds: %w", err)
		}
	}

	decimal, err := getParameterValueWithName[int](ctx, parameters, "decimal")
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

	limiter, err := validateNoiseFloatTypeAndSetLimit(
		ctx, typeSize, minValueThreshold, maxValueThreshold, decimal,
	)
	if err != nil {
		return nil, fmt.Errorf("validate float type and set limits: %w", err)
	}

	t := transformers.NewNoiseFloat64Transformer(limiter, minRatio, maxRatio)

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &NoiseFloatTransformer{
		t:          t,
		columnName: columnName,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx:   column.Idx,
		decimal:     decimal,
		minParam:    minParam,
		maxParam:    maxParam,
		dynamicMode: dynamicMode,
		floatSize:   typeSize,
		transform: func(f float64) (float64, error) {
			return t.Transform(nil, f)
		},
	}, nil
}

func (t *NoiseFloatTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *NoiseFloatTransformer) Init(context.Context) error {
	if t.dynamicMode {
		t.transform = t.dynamicTransform
	}
	return nil
}

func (t *NoiseFloatTransformer) Done(context.Context) error {
	return nil
}

func (t *NoiseFloatTransformer) dynamicTransform(v float64) (float64, error) {
	minVal, maxVal, err := getNoiseFloatMinAndMaxThresholds[float64](t.floatSize, t.minParam, t.maxParam, getFloatLimits)
	if err != nil {
		return 0, fmt.Errorf("unable to get min and max values: %w", err)
	}

	limiter, err := transformers.NewNoiseFloat64Limiter(minVal, maxVal, t.decimal)
	if err != nil {
		return 0, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	res, err := t.t.Transform(limiter, v)
	if err != nil {
		return 0, fmt.Errorf("error generating int value: %w", err)
	}
	return res, nil
}

func (t *NoiseFloatTransformer) Transform(_ context.Context, r interfaces.Recorder) error {
	var val float64
	isNull, err := r.ScanColumnValueByIdx(t.columnIdx, &val)
	if err != nil {
		return fmt.Errorf("unable to scan value: %w", err)
	}
	if isNull {
		return nil
	}

	res, err := t.transform(val)
	if err != nil {
		return fmt.Errorf("unable to transform value: %w", err)
	}

	if err = r.SetColumnValueByIdx(t.columnIdx, res); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}
	return nil
}

func (t *NoiseFloatTransformer) Describe() string {
	return TransformerNameNoiseFloat
}

func validateNoiseFloatTypeAndSetLimit(
	ctx context.Context,
	size int,
	requestedMinValue,
	requestedMaxValue *float64,
	decimal int,
) (*transformers.NoiseFloat64Limiter, error) {
	minValue, maxValue, err := getFloatLimits(size)
	if err != nil {
		return nil, fmt.Errorf("get float%d limits: %w", size, err)
	}
	if requestedMinValue == nil {
		requestedMinValue = &minValue
	}
	if requestedMaxValue == nil {
		requestedMaxValue = &maxValue
	}

	if !isValueInLimits(*requestedMinValue, minValue, maxValue) {
		validationcollector.FromContext(ctx).Add(
			models.NewValidationWarning().
				SetMsgf("requested min value is out of float%d range", size).
				SetSeverity(models.ValidationSeverityError).
				AddMeta("AllowedMinValue", minValue).
				AddMeta("AllowedMaxValue", maxValue).
				AddMeta("ParameterName", "min").
				AddMeta("ParameterValue", requestedMinValue),
		)
		return nil, models.ErrFatalValidationError
	}

	if !isValueInLimits(*requestedMaxValue, minValue, maxValue) {
		validationcollector.FromContext(ctx).Add(
			models.NewValidationWarning().
				SetMsgf("requested max value is out of float%d range", size).
				SetSeverity(models.ValidationSeverityError).
				AddMeta("AllowedMinValue", minValue).
				AddMeta("AllowedMaxValue", maxValue).
				AddMeta("ParameterName", "min").
				AddMeta("ParameterValue", requestedMinValue),
		)
		return nil, models.ErrFatalValidationError
	}

	return transformers.NewNoiseFloat64Limiter(*requestedMinValue, *requestedMaxValue, decimal)
}
