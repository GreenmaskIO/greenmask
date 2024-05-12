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

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	float4Length = 4
	float8Length = 8
)

var floatTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"RandomFloat",
		"Generate float value in min and max thresholds and round up to provided digits",
	),

	NewFloatTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("float4", "float8"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"min",
		"min float value threshold",
	).SetRequired(true).
		SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("float4", "float8"),
		),

	toolkit.MustNewParameterDefinition(
		"max",
		"max float value threshold",
	).SetRequired(true).
		SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("float4", "float8"),
		),

	toolkit.MustNewParameterDefinition(
		"precision",
		"precision of float value (number of digits after coma)",
	).SetDefaultValue(toolkit.ParamsValue("4")),

	keepNullParameterDefinition,

	engineParameterDefinition,
)

type FloatTransformer struct {
	t               *transformers.RandomFloat64Transformer
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
	columnIdx       int
	dynamicMode     bool
	floatSize       int
	precision       int

	columnParam    toolkit.Parameterizer
	maxParam       toolkit.Parameterizer
	minParam       toolkit.Parameterizer
	keepNullParam  toolkit.Parameterizer
	engineParam    toolkit.Parameterizer
	precisionParam toolkit.Parameterizer

	transform func(context.Context, []byte) (float64, error)
}

func NewFloatTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName, engine string
	var minVal, maxVal float64
	var keepNull, dynamicMode bool
	var precision int
	floatSize := 8

	columnParam := parameters["column"]
	minParam := parameters["min"]
	maxParam := parameters["max"]
	keepNullParam := parameters["keep_null"]
	engineParam := parameters["engine"]
	precisionParam := parameters["precision"]

	if err := engineParam.Scan(&engine); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "engine" param: %w`, err)
	}

	if minParam.IsDynamic() || maxParam.IsDynamic() {
		dynamicMode = true
	}

	if err := columnParam.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, c, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName
	if c.Length != -1 {
		floatSize = c.Length
	}

	if err := keepNullParam.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	if !dynamicMode {
		if err := minParam.Scan(&minVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"min\" parameter: %w", err)
		}
		if err := maxParam.Scan(&maxVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"max\" parameter: %w", err)
		}
	}

	if err := precisionParam.Scan(&precision); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "precision" param: %w`, err)
	}

	limiter, limitsWarnings, err := validateFloatTypeAndSetLimit(floatSize, minVal, maxVal, precision)
	if err != nil {
		return nil, nil, err
	}
	if limitsWarnings.IsFatal() {
		return nil, limitsWarnings, nil
	}

	t := transformers.NewRandomFloat64Transformer(limiter)

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &FloatTransformer{
		t:               t,
		columnName:      columnName,
		keepNull:        keepNull,
		affectedColumns: affectedColumns,
		columnIdx:       idx,
		precision:       precision,

		columnParam:    columnParam,
		minParam:       minParam,
		maxParam:       maxParam,
		keepNullParam:  keepNullParam,
		engineParam:    engineParam,
		precisionParam: precisionParam,

		dynamicMode: dynamicMode,
		floatSize:   floatSize,

		transform: t.Transform,
	}, nil, nil
}

func (rit *FloatTransformer) GetAffectedColumns() map[int]string {
	return rit.affectedColumns
}

func (rit *FloatTransformer) Init(ctx context.Context) error {
	if rit.dynamicMode {
		rit.transform = rit.dynamicTransform
	}
	return nil
}

func (rit *FloatTransformer) Done(ctx context.Context) error {
	return nil
}

func (rit *FloatTransformer) dynamicTransform(ctx context.Context, v []byte) (float64, error) {

	var minVal, maxVal float64
	err := rit.minParam.Scan(&minVal)
	if err != nil {
		return 0, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	err = rit.maxParam.Scan(&maxVal)
	if err != nil {
		return 0, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	limiter, err := getFloat64LimiterForDynamicParameter(rit.floatSize, minVal, maxVal, rit.precision)
	if err != nil {
		return 0, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	ctx = context.WithValue(ctx, "limiter", limiter)
	res, err := rit.t.Transform(ctx, v)
	if err != nil {
		return 0, fmt.Errorf("error generating float value: %w", err)
	}
	return res, nil
}

func (rit *FloatTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(rit.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rit.keepNull {
		return r, nil
	}

	newVal, err := rit.transform(ctx, val.Data)
	if err != nil {
		return nil, err
	}

	if err = r.SetColumnValueByIdx(rit.columnIdx, newVal); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
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

func getFloat64LimiterForDynamicParameter(size int, requestedMinValue, requestedMaxValue float64, precision int) (*transformers.Float64Limiter, error) {
	minValue, maxValue, err := getFloatThresholds(size)
	if err != nil {
		return nil, err
	}

	if !limitIsValid(requestedMinValue, minValue, maxValue) {
		return nil, fmt.Errorf("requested dynamic parameter min value is out of range of float%d size", size)
	}

	if !limitIsValid(requestedMaxValue, minValue, maxValue) {
		return nil, fmt.Errorf("requested dynamic parameter max value is out of range of float%d size", size)
	}

	limiter, err := transformers.NewFloat64Limiter(-math.MaxFloat64, math.MaxFloat64, precision)
	if err != nil {
		return nil, err
	}

	if requestedMinValue != 0 || requestedMaxValue != 0 {
		limiter, err = transformers.NewFloat64Limiter(requestedMinValue, requestedMaxValue, precision)
		if err != nil {
			return nil, err
		}
	}
	return limiter, nil
}

func validateFloatTypeAndSetLimit(
	size int, requestedMinValue, requestedMaxValue float64, precision int,
) (limiter *transformers.Float64Limiter, warns toolkit.ValidationWarnings, err error) {

	minValue, maxValue, err := getFloatThresholds(size)
	if err != nil {
		return nil, nil, err
	}

	if !limitIsValid(requestedMinValue, minValue, maxValue) {
		warns = append(warns, toolkit.NewValidationWarning().
			SetMsgf("requested min value is out of float%d range", size).
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("AllowedMinValue", minValue).
			AddMeta("AllowedMaxValue", maxValue).
			AddMeta("ParameterName", "min").
			AddMeta("ParameterValue", requestedMinValue),
		)
	}

	if !limitIsValid(requestedMaxValue, minValue, maxValue) {
		warns = append(warns, toolkit.NewValidationWarning().
			SetMsgf("requested max value is out of float%d range", size).
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("AllowedMinValue", minValue).
			AddMeta("AllowedMaxValue", maxValue).
			AddMeta("ParameterName", "min").
			AddMeta("ParameterValue", requestedMinValue),
		)
	}

	if warns.IsFatal() {
		return nil, warns, nil
	}

	limiter, err = transformers.NewFloat64Limiter(-math.MaxFloat64, math.MaxFloat64, precision)
	if err != nil {
		return nil, nil, err
	}

	if requestedMinValue != 0 || requestedMaxValue != 0 {
		limiter, err = transformers.NewFloat64Limiter(requestedMinValue, requestedMaxValue, precision)
		if err != nil {
			return nil, nil, err
		}
	}

	return limiter, nil, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(floatTransformerDefinition)
}
