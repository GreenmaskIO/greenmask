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
	"math"

	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/rs/zerolog/log"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const RandomFloatTransformerName = "RandomFloat"

var RamdomFloatTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		RandomFloatTransformerName,
		"Generate float value in min and max thresholds and round up to provided digits",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewFloatTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		commonparameters.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("float4", "float8", "float"),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"min",
		"min float value threshold",
	).SetSupportTemplate(true).
		LinkParameter("column").
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetCompatibleTypes("float4", "float8", "float"),
		),

	commonparameters.MustNewParameterDefinition(
		"max",
		"max float value threshold",
	).SetSupportTemplate(true).
		LinkParameter("column").
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetCompatibleTypes("float4", "float8", "float"),
		),

	commonparameters.MustNewParameterDefinition(
		"decimal",
		"Numbers of decimal",
	).SetSupportTemplate(true).
		SetDefaultValue(commonmodels.ParamsValue("4")),

	defaultFloatTypeSizeParameterDefinition,

	defaultKeepNullParameterDefinition,

	defaultEngineParameterDefinition,
)

type FloatTransformer struct {
	t               *transformers.RandomFloat64Transformer
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
	columnIdx       int
	dynamicMode     bool
	floatSize       int
	decimal         int

	maxParam commonparameters.Parameterizer
	minParam commonparameters.Parameterizer

	transform func([]byte) (float64, error)
}

func NewFloatTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	minParam := parameters["min"]
	maxParam := parameters["max"]

	var minVal, maxVal *float64

	dynamicMode := isInDynamicMode(parameters)

	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	keepNull, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameKeepNull)
	if err != nil {
		return nil, fmt.Errorf("get \"keep_null\" param: %w", err)
	}

	engine, err := getParameterValueWithName[string](ctx, parameters, ParameterNameEngine)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	typeSize := column.Length
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

	if !dynamicMode {
		minVal, maxVal, err = getOptionalMinAndMaxThresholds[float64](minParam, maxParam)
		if err != nil {
			return nil, fmt.Errorf("get min and max thresholds: %w", err)
		}
	}

	decimal, err := getParameterValueWithName[int](ctx, parameters, "decimal")
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	limiter, err := validateFloatTypeAndSetLimit(ctx, typeSize, minVal, maxVal, decimal)
	if err != nil {
		return nil, err
	}

	t := transformers.NewRandomFloat64Transformer(limiter)

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("set generator: %w", err)
	}

	return &FloatTransformer{
		t:          t,
		columnName: columnName,
		keepNull:   keepNull,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx: column.Idx,
		decimal:   decimal,

		minParam: minParam,
		maxParam: maxParam,

		dynamicMode: dynamicMode,
		floatSize:   typeSize,

		transform: func(bytes []byte) (float64, error) {
			return t.Transform(nil, bytes)
		},
	}, nil
}

func (rit *FloatTransformer) GetAffectedColumns() map[int]string {
	return rit.affectedColumns
}

func (rit *FloatTransformer) Init(_ context.Context) error {
	if rit.dynamicMode {
		rit.transform = rit.dynamicTransform
	}
	return nil
}

func (rit *FloatTransformer) Done(_ context.Context) error {
	return nil
}

func (rit *FloatTransformer) dynamicTransform(v []byte) (float64, error) {

	var minVal, maxVal float64
	err := rit.minParam.Scan(&minVal)
	if err != nil {
		return 0, fmt.Errorf(`scan "min" param: %w`, err)
	}

	err = rit.maxParam.Scan(&maxVal)
	if err != nil {
		return 0, fmt.Errorf(`scan "max" param: %w`, err)
	}

	limiter, err := getFloat64LimiterForDynamicParameter(rit.floatSize, minVal, maxVal, rit.decimal)
	if err != nil {
		return 0, fmt.Errorf("create limiter in dynamic mode: %w", err)
	}
	res, err := rit.t.Transform(limiter, v)
	if err != nil {
		return 0, fmt.Errorf("generate float value: %w", err)
	}
	return res, nil
}

func (rit *FloatTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	val, err := r.GetRawColumnValueByIdx(rit.columnIdx)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if val.IsNull && rit.keepNull {
		return nil
	}

	newVal, err := rit.transform(val.Data)
	if err != nil {
		return err
	}

	if err = r.SetColumnValueByIdx(rit.columnIdx, newVal); err != nil {
		return fmt.Errorf("set new value: %w", err)
	}
	return nil
}

func getFloatThresholds(size int) (float64, float64, error) {
	switch size {
	case float4Length:
		return -math.MaxFloat32, math.MaxFloat32, nil
	case float8Length:
		return -math.MaxFloat64, math.MaxFloat64, nil
	}

	return 0, 0, fmt.Errorf("unsupported float size %d", size)
}

func getFloat64LimiterForDynamicParameter(size int, requestedMinValue, requestedMaxValue float64, decimal int) (*transformers.Float64Limiter, error) {
	minValue, maxValue, err := getFloatThresholds(size)
	if err != nil {
		return nil, err
	}

	if !isValueInLimits(requestedMinValue, minValue, maxValue) {
		return nil, fmt.Errorf("requested dynamic parameter min value is out of range of float%d size", size)
	}

	if !isValueInLimits(requestedMaxValue, minValue, maxValue) {
		return nil, fmt.Errorf("requested dynamic parameter max value is out of range of float%d size", size)
	}

	limiter, err := transformers.NewFloat64Limiter(-math.MaxFloat64, math.MaxFloat64, decimal)
	if err != nil {
		return nil, err
	}

	if requestedMinValue != 0 || requestedMaxValue != 0 {
		limiter, err = transformers.NewFloat64Limiter(requestedMinValue, requestedMaxValue, decimal)
		if err != nil {
			return nil, err
		}
	}
	return limiter, nil
}

func validateFloatTypeAndSetLimit(
	ctx context.Context, size int, requestedMinValue, requestedMaxValue *float64, decimal int,
) (limiter *transformers.Float64Limiter, err error) {
	minValue, maxValue, err := getFloatThresholds(size)
	if err != nil {
		return nil, err
	}

	if requestedMinValue == nil {
		requestedMinValue = &minValue
	}
	if requestedMaxValue == nil {
		requestedMaxValue = &maxValue
	}

	if !isValueInLimits(*requestedMinValue, minValue, maxValue) {
		validationcollector.FromContext(ctx).Add(
			commonmodels.NewValidationWarning().
				SetMsgf("requested min value is out of float%d range", size).
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("AllowedMinValue", minValue).
				AddMeta("AllowedMaxValue", maxValue).
				AddMeta("ParameterName", "min").
				AddMeta("ParameterValue", requestedMinValue))
		return nil, commonmodels.ErrFatalValidationError
	}

	if !isValueInLimits(*requestedMaxValue, minValue, maxValue) {
		validationcollector.FromContext(ctx).Add(
			commonmodels.NewValidationWarning().
				SetMsgf("requested max value is out of float%d range", size).
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("AllowedMinValue", minValue).
				AddMeta("AllowedMaxValue", maxValue).
				AddMeta("ParameterName", "max").
				AddMeta("ParameterValue", requestedMaxValue))
		return nil, commonmodels.ErrFatalValidationError
	}

	limiter, err = transformers.NewFloat64Limiter(-math.MaxFloat64, math.MaxFloat64, decimal)
	if err != nil {
		return nil, fmt.Errorf("create limiter by size: %w", err)
	}

	limiter, err = transformers.NewFloat64Limiter(*requestedMinValue, *requestedMaxValue, decimal)
	if err != nil {
		return nil, fmt.Errorf("create limiter by max: %w", err)
	}

	return limiter, nil
}
