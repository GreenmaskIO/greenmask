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

	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/v1/pkg/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/pkg/common/models"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/utils"
	"github.com/rs/zerolog/log"
)

const TransformerNameRandomInt = "RandomInt"

var RandomIntegerTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TransformerNameRandomInt,
		"Generate integer value in min and max thresholds",
	).AddMeta(utils.AllowApplyForReferenced, true).
		AddMeta(utils.RequireHashEngineParameter, true),

	NewIntegerTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		parameters.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypeClasses(commonmodels.TypeClassInt),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"min",
		"min int value threshold",
	).LinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			parameters.NewDynamicModeProperties().
				SetColumnProperties(
					parameters.NewColumnProperties().
						SetAllowedColumnTypeClasses(commonmodels.TypeClassInt),
				),
		),

	parameters.MustNewParameterDefinition(
		"max",
		"max int value threshold",
	).LinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			parameters.NewDynamicModeProperties().
				SetColumnProperties(
					parameters.NewColumnProperties().
						SetAllowedColumnTypeClasses(commonmodels.TypeClassInt),
				),
		),

	defaultIntTypeSizeParameterDefinition,

	defaultKeepNullParameterDefinition,

	defaultEngineParameterDefinition,
)

type IntegerTransformer struct {
	*transformers.RandomInt64Transformer
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
	columnIdx       int
	dynamicMode     bool
	intSize         int

	maxParam parameters.Parameterizer
	minParam parameters.Parameterizer

	transform func([]byte) (int64, error)
}

func NewIntegerTransformer(
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
) (interfaces.Transformer, error) {
	var minVal, maxVal *int64

	maxParam := parameters["max"]
	minParam := parameters["min"]

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
		minVal, maxVal, err = getOptionalMinAndMaxThresholds[int64](minParam, maxParam)
		if err != nil {
			return nil, fmt.Errorf("get min and max thresholds: %w", err)
		}
	}

	limiter, err := validateIntTypeAndSetRandomInt64Limiter(ctx, typeSize, minVal, maxVal)
	if err != nil {
		return nil, err
	}

	t, err := transformers.NewRandomInt64Transformer(limiter, typeSize)
	if err != nil {
		return nil, fmt.Errorf("error initializing common int transformer: %w", err)
	}

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &IntegerTransformer{
		RandomInt64Transformer: t,
		columnName:             columnName,
		keepNull:               keepNull,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx: column.Idx,

		minParam: minParam,
		maxParam: maxParam,

		dynamicMode: dynamicMode,
		intSize:     typeSize,

		transform: func(bytes []byte) (int64, error) {
			return t.Transform(nil, bytes)
		},
	}, nil
}

func (t *IntegerTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *IntegerTransformer) Init(context.Context) error {
	if t.dynamicMode {
		t.transform = t.dynamicTransform
	}
	return nil
}

func (t *IntegerTransformer) Done(context.Context) error {
	return nil
}

func (t *IntegerTransformer) dynamicTransform(v []byte) (int64, error) {

	var minVal, maxVal int64
	err := t.minParam.Scan(&minVal)
	if err != nil {
		return 0, fmt.Errorf(`scan "min" param: %w`, err)
	}

	err = t.maxParam.Scan(&maxVal)
	if err != nil {
		return 0, fmt.Errorf(`scan "max" param: %w`, err)
	}

	limiter, err := getRandomInt64LimiterForDynamicParameter(t.intSize, minVal, maxVal)
	if err != nil {
		return 0, fmt.Errorf("create limiter in dynamic mode: %w", err)
	}
	res, err := t.RandomInt64Transformer.Transform(limiter, v)
	if err != nil {
		return 0, fmt.Errorf("generate int value: %w", err)
	}
	return res, nil
}

func (t *IntegerTransformer) Transform(_ context.Context, r interfaces.Recorder) error {
	val, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if val.IsNull && t.keepNull {
		return nil
	}

	newVal, err := t.transform(val.Data)
	if err != nil {
		return err
	}

	if err = r.SetColumnValueByIdx(t.columnIdx, newVal); err != nil {
		return fmt.Errorf("set new value: %w", err)
	}
	return nil
}

func (t *IntegerTransformer) Describe() string {
	return TransformerNameRandomInt
}

func getRandomInt64LimiterForDynamicParameter(size int, requestedMinValue, requestedMaxValue int64) (*transformers.Int64Limiter, error) {
	minValue, maxValue, err := getIntThresholds(size)
	if err != nil {
		return nil, err
	}

	if !isValueInLimits(requestedMinValue, minValue, maxValue) {
		return nil, fmt.Errorf("requested dynamic parameter min value is out of range of int%d size", size)
	}

	if !isValueInLimits(requestedMaxValue, minValue, maxValue) {
		return nil, fmt.Errorf("requested dynamic parameter max value is out of range of int%d size", size)
	}

	limiter, err := transformers.NewInt64Limiter(minValue, maxValue)
	if err != nil {
		return nil, err
	}

	if requestedMinValue != 0 || requestedMaxValue != 0 {
		limiter, err = transformers.NewInt64Limiter(requestedMinValue, requestedMaxValue)
		if err != nil {
			return nil, err
		}
	}
	return limiter, nil
}

func validateIntTypeAndSetRandomInt64Limiter(
	ctx context.Context, size int, requestedMinValue, requestedMaxValue *int64,
) (*transformers.Int64Limiter, error) {

	minValue, maxValue, err := validateInt64AndGetLimits(ctx, size, requestedMinValue, requestedMaxValue)
	if err != nil {
		return nil, err
	}
	l, err := transformers.NewInt64Limiter(minValue, maxValue)
	if err != nil {
		return nil, err
	}
	return l, nil
}
