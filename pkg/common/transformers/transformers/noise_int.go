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

const TransformerNameNoiseInt = "NoiseInt"

var NoiseIntTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TransformerNameNoiseInt,
		"Add noise to int in min and max thresholds",
	).AddMeta(utils.AllowApplyForReferenced, true).
		AddMeta(utils.RequireHashEngineParameter, true),

	NewNoiseIntTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(models.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypeClasses(models.TypeClassInt),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"min",
		"min value threshold limiter",
	).SetSupportTemplate(true).
		SetDynamicMode(
			parameters.NewDynamicModeProperties().
				SetColumnProperties(
					models.NewColumnProperties().
						SetAllowedColumnTypeClasses(models.TypeClassInt),
				),
		),

	parameters.MustNewParameterDefinition(
		"max",
		"max value threshold limiter",
	).SetSupportTemplate(true).
		SetDynamicMode(
			parameters.NewDynamicModeProperties().
				SetColumnProperties(
					models.NewColumnProperties().
						SetAllowedColumnTypeClasses(models.TypeClassInt),
				),
		),

	defaultIntTypeSizeParameterDefinition,

	defaultMinRatioParameterDefinition,

	defaultMaxRatioParameterDefinition,

	defaultEngineParameterDefinition,
)

type NoiseIntTransformer struct {
	t               *transformers.NoiseInt64Transformer
	columnName      string
	columnIdx       int
	affectedColumns map[int]string
	intSize         int
	dynamicMode     bool
	maxParam        parameters.Parameterizer
	minParam        parameters.Parameterizer
	transform       func(int64) (int64, error)
}

func NewNoiseIntTransformer(
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
) (interfaces.Transformer, error) {
	var maxValueThreshold, minValueThreshold *int64

	maxParam := parameters["max"]
	minParam := parameters["min"]

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
			Warn().
			Msg("unable to detect float size from column length, trying to it from \"type_size\" parameter")
		typeSize, err = getParameterValueWithName[int](
			ctx,
			parameters,
			"type_size",
		)
		if err != nil {
			return nil, fmt.Errorf("unable to scan \"type_size\" param: %w", err)
		}
		log.Warn().Msgf("using float size %d from \"type_size\" parameter", typeSize)
	}

	if !dynamicMode {
		minValueThreshold, maxValueThreshold, err = getOptionalMinAndMaxThresholds[int64](minParam, maxParam)
		if err != nil {
			return nil, fmt.Errorf("get min and max thresholds: %w", err)
		}
	}

	limiter, err := validateIntTypeAndSetNoiseInt64Limiter(ctx, typeSize, minValueThreshold, maxValueThreshold)
	if err != nil {
		return nil, fmt.Errorf("validate int type and set limits: %w", err)
	}

	minRatio, err := getParameterValueWithName[float64](ctx, parameters, "min_ratio")
	if err != nil {
		return nil, fmt.Errorf("get \"max_ratio\" param: %w", err)
	}
	maxRatio, err := getParameterValueWithName[float64](ctx, parameters, "max_ratio")
	if err != nil {
		return nil, fmt.Errorf("get \"max_ratio\" param: %w", err)
	}

	t, err := transformers.NewNoiseInt64Transformer(limiter, minRatio, maxRatio)
	if err != nil {
		return nil, fmt.Errorf("error initializing common int transformer: %w", err)
	}

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("set generator: %w", err)
	}

	return &NoiseIntTransformer{
		t:               t,
		dynamicMode:     dynamicMode,
		columnName:      columnName,
		affectedColumns: map[int]string{column.Idx: columnName},
		columnIdx:       column.Idx,
		intSize:         typeSize,
		minParam:        minParam,
		maxParam:        maxParam,
		transform: func(i int64) (int64, error) {
			return t.Transform(nil, i)
		},
	}, nil
}

func (t *NoiseIntTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *NoiseIntTransformer) Init(context.Context) error {
	if t.dynamicMode {
		t.transform = t.dynamicTransform
	}
	return nil
}

func (t *NoiseIntTransformer) Done(context.Context) error {
	return nil
}

func (t *NoiseIntTransformer) dynamicTransform(v int64) (int64, error) {
	minVal, maxVal, err := getMinAndMaxIntDynamicValueNoiseIntTrans(t.intSize, t.minParam, t.maxParam)
	if err != nil {
		return 0, fmt.Errorf("unable to get min and max values: %w", err)
	}

	limiter, err := transformers.NewNoiseInt64Limiter(minVal, maxVal)
	if err != nil {
		return 0, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	res, err := t.t.Transform(limiter, v)
	if err != nil {
		return 0, fmt.Errorf("error generating int value: %w", err)
	}
	return res, nil
}

func (t *NoiseIntTransformer) Transform(_ context.Context, r interfaces.Recorder) error {
	var val int64
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

func (t *NoiseIntTransformer) Describe() string {
	return TransformerNameNoiseInt
}

func validateIntTypeAndSetNoiseInt64Limiter(
	ctx context.Context, size int, requestedMinValue, requestedMaxValue *int64,
) (*transformers.NoiseInt64Limiter, error) {
	minValue, maxValue, err := validateInt64AndGetLimits(ctx, size, requestedMinValue, requestedMaxValue)
	if err != nil {
		return nil, err
	}
	return transformers.NewNoiseInt64Limiter(minValue, maxValue)
}

func getMinAndMaxIntDynamicValueNoiseIntTrans(intSize int, minParam, maxParam parameters.Parameterizer) (int64, int64, error) {
	var requestedMinValue, requestedMaxValue int64
	var minRequested, maxRequested bool
	minValue, maxValue, err := getIntThresholds(intSize)
	if err != nil {
		return 0, 0, err
	}

	if minParam.IsDynamic() {
		minRequested = true
		err = minParam.Scan(&requestedMinValue)
		if err != nil {
			return 0, 0, fmt.Errorf(`unable to scan "min" dynamic  param: %w`, err)
		}
		if !isValueInLimits(requestedMinValue, minValue, maxValue) {
			return 0, 0, fmt.Errorf("requested dynamic parameter min value is out of range of int%d size", intSize)
		}
	}

	if maxParam.IsDynamic() {
		maxRequested = true
		err = maxParam.Scan(&requestedMaxValue)
		if err != nil {
			return 0, 0, fmt.Errorf(`unable to scan "max" dynamic param: %w`, err)
		}
		if !isValueInLimits(requestedMaxValue, minValue, maxValue) {
			return 0, 0, fmt.Errorf("requested dynamic parameter max value is out of range of int%d size", intSize)
		}
	}

	if minRequested {
		minValue = requestedMinValue
	}
	if maxRequested {
		maxValue = requestedMaxValue
	}

	return minValue, maxValue, nil
}

func validateInt64AndGetLimits(
	ctx context.Context, size int, requestedMinValue, requestedMaxValue *int64,
) (int64, int64, error) {
	minValue, maxValue, err := getIntThresholds(size)
	if err != nil {
		return 0, 0, err
	}
	if requestedMinValue == nil {
		requestedMinValue = &minValue
	}
	if requestedMaxValue == nil {
		requestedMaxValue = &maxValue
	}

	if !isValueInLimits(*requestedMinValue, minValue, maxValue) {
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				AddMeta("AllowedMinValue", minValue).
				AddMeta("AllowedMaxValue", maxValue).
				AddMeta("ParameterName", "min").
				AddMeta("ParameterValue", requestedMinValue).
				SetMsgf("requested min value is out of int%d range", size))
		return 0, 0, models.ErrFatalValidationError
	}

	return *requestedMinValue, *requestedMaxValue, nil
}
