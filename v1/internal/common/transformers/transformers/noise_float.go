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

	"github.com/rs/zerolog/log"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/generators/transformers"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const (
	NoiseFloatTransformerName = "NoiseFloat"

	float4Length = 4
	float8Length = 8
)

var NoiseFloatTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		NoiseFloatTransformerName,
		"Add noise to float value in min and max thresholds",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewNoiseFloatTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("float4", "float8", "float").
		SetSkipOnNull(true),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"decimal",
		"Number of decimal places to use",
	).SetSupportTemplate(true).
		SetDefaultValue(commonmodels.ParamsValue("4")),

	commonparameters.MustNewParameterDefinition(
		"min",
		"min float value threshold",
	).LinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetCompatibleTypes(
					"float4", "float8", "int2", "int4", "int8",
					"float",
				),
		),

	commonparameters.MustNewParameterDefinition(
		"max",
		"max float value threshold",
	).LinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetCompatibleTypes("float4", "float8", "int2", "int4", "int8"),
		),

	commonparameters.MustNewParameterDefinition(
		"type_size",
		"float size (4 or 8). It is used if greenmask can't detect it from column length",
	).SetDefaultValue(commonmodels.ParamsValue("4")),

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

	maxParam commonparameters.Parameterizer
	minParam commonparameters.Parameterizer

	transform func(float64) (float64, error)
}

func NewNoiseFloatTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
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

func (nft *NoiseFloatTransformer) GetAffectedColumns() map[int]string {
	return nft.affectedColumns
}

func (nft *NoiseFloatTransformer) Init(context.Context) error {
	if nft.dynamicMode {
		nft.transform = nft.dynamicTransform
	}
	return nil
}

func (nft *NoiseFloatTransformer) Done(context.Context) error {
	return nil
}

func (nft *NoiseFloatTransformer) dynamicTransform(v float64) (float64, error) {
	minVal, maxVal, err := getNoiseFloatMinAndMaxThresholds[float64](nft.floatSize, nft.minParam, nft.maxParam, getFloatLimits)
	if err != nil {
		return 0, fmt.Errorf("unable to get min and max values: %w", err)
	}

	limiter, err := transformers.NewNoiseFloat64Limiter(minVal, maxVal, nft.decimal)
	if err != nil {
		return 0, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	res, err := nft.t.Transform(limiter, v)
	if err != nil {
		return 0, fmt.Errorf("error generating int value: %w", err)
	}
	return res, nil
}

func (nft *NoiseFloatTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	var val float64
	isNull, err := r.ScanColumnValueByIdx(nft.columnIdx, &val)
	if err != nil {
		return fmt.Errorf("unable to scan value: %w", err)
	}
	if isNull {
		return nil
	}

	res, err := nft.transform(val)
	if err != nil {
		return fmt.Errorf("unable to transform value: %w", err)
	}

	if err = r.SetColumnValueByIdx(nft.columnIdx, res); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}
	return nil
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
			commonmodels.NewValidationWarning().
				SetMsgf("requested min value is out of float%d range", size).
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("AllowedMinValue", minValue).
				AddMeta("AllowedMaxValue", maxValue).
				AddMeta("ParameterName", "min").
				AddMeta("ParameterValue", requestedMinValue),
		)
		return nil, commonmodels.ErrFatalValidationError
	}

	if !isValueInLimits(*requestedMaxValue, minValue, maxValue) {
		validationcollector.FromContext(ctx).Add(
			commonmodels.NewValidationWarning().
				SetMsgf("requested max value is out of float%d range", size).
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("AllowedMinValue", minValue).
				AddMeta("AllowedMaxValue", maxValue).
				AddMeta("ParameterName", "min").
				AddMeta("ParameterValue", requestedMinValue),
		)
		return nil, commonmodels.ErrFatalValidationError
	}

	return transformers.NewNoiseFloat64Limiter(*requestedMinValue, *requestedMaxValue, decimal)
}
