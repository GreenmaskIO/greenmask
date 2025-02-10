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
	"github.com/greenmaskio/greenmask/pkg/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	Int2Length = 2
	Int4Length = 4
	Int8Length = 8
)

const RandomIntTransformerName = "RandomInt"

var integerTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		RandomIntTransformerName,
		"Generate integer value in min and max thresholds",
	).AddMeta(AllowApplyForReferenced, true).
		AddMeta(RequireHashEngineParameter, true),

	NewIntegerTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("int2", "int4", "int8"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"min",
		"min int value threshold",
	).SetLinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("int2", "int4", "int8"),
		),

	toolkit.MustNewParameterDefinition(
		"max",
		"max int value threshold",
	).SetLinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("int2", "int4", "int8"),
		),

	keepNullParameterDefinition,

	engineParameterDefinition,
)

type IntegerTransformer struct {
	*transformers.RandomInt64Transformer
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
	columnIdx       int
	dynamicMode     bool
	intSize         int

	columnParam   toolkit.Parameterizer
	maxParam      toolkit.Parameterizer
	minParam      toolkit.Parameterizer
	keepNullParam toolkit.Parameterizer
	engineParam   toolkit.Parameterizer

	transform func([]byte) (int64, error)
}

func NewIntegerTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName, engine string
	var minVal, maxVal *int64
	var keepNull, dynamicMode bool

	columnParam := parameters["column"]
	minParam := parameters["min"]
	maxParam := parameters["max"]
	keepNullParam := parameters["keep_null"]
	engineParam := parameters["engine"]

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
	intSize := c.GetColumnSize()

	if err := keepNullParam.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	if !dynamicMode {
		minIsEmpty, err := minParam.IsEmpty()
		if err != nil {
			return nil, nil, fmt.Errorf("error checking \"min\" parameter: %w", err)
		}
		if !minIsEmpty {
			if err = minParam.Scan(&minVal); err != nil {
				return nil, nil, fmt.Errorf("error scanning \"min\" parameter: %w", err)
			}
		}
		maxIsEmpty, err := maxParam.IsEmpty()
		if err != nil {
			return nil, nil, fmt.Errorf("error checking \"max\" parameter: %w", err)
		}
		if !maxIsEmpty {
			if err = maxParam.Scan(&maxVal); err != nil {
				return nil, nil, fmt.Errorf("error scanning \"max\" parameter: %w", err)
			}
		}
	}

	limiter, limitsWarnings, err := validateIntTypeAndSetRandomInt64Limiter(intSize, minVal, maxVal)
	if err != nil {
		return nil, nil, err
	}
	if limitsWarnings.IsFatal() {
		return nil, limitsWarnings, nil
	}

	t, err := transformers.NewRandomInt64Transformer(limiter, intSize)
	if err != nil {
		return nil, nil, fmt.Errorf("error initializing common int transformer: %w", err)
	}

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &IntegerTransformer{
		RandomInt64Transformer: t,
		columnName:             columnName,
		keepNull:               keepNull,
		affectedColumns:        affectedColumns,
		columnIdx:              idx,

		columnParam:   columnParam,
		minParam:      minParam,
		maxParam:      maxParam,
		keepNullParam: keepNullParam,
		engineParam:   engineParam,

		dynamicMode: dynamicMode,
		intSize:     intSize,

		transform: func(bytes []byte) (int64, error) {
			return t.Transform(nil, bytes)
		},
	}, nil, nil
}

func (rit *IntegerTransformer) GetAffectedColumns() map[int]string {
	return rit.affectedColumns
}

func (rit *IntegerTransformer) Init(_ context.Context) error {
	if rit.dynamicMode {
		rit.transform = rit.dynamicTransform
	}
	return nil
}

func (rit *IntegerTransformer) Done(_ context.Context) error {
	return nil
}

func (rit *IntegerTransformer) dynamicTransform(v []byte) (int64, error) {

	var minVal, maxVal int64
	err := rit.minParam.Scan(&minVal)
	if err != nil {
		return 0, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	err = rit.maxParam.Scan(&maxVal)
	if err != nil {
		return 0, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	limiter, err := getRandomInt64LimiterForDynamicParameter(rit.intSize, minVal, maxVal)
	if err != nil {
		return 0, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	res, err := rit.RandomInt64Transformer.Transform(limiter, v)
	if err != nil {
		return 0, fmt.Errorf("error generating int value: %w", err)
	}
	return res, nil
}

func (rit *IntegerTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(rit.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rit.keepNull {
		return r, nil
	}

	newVal, err := rit.transform(val.Data)
	if err != nil {
		return nil, err
	}

	if err = r.SetColumnValueByIdx(rit.columnIdx, newVal); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func getIntThresholds(size int) (int64, int64, error) {
	switch size {
	case Int2Length:
		return math.MinInt16, math.MaxInt16, nil
	case Int4Length:
		return math.MinInt32, math.MaxInt32, nil
	case Int8Length:
		return math.MinInt16, math.MaxInt16, nil
	}

	return 0, 0, fmt.Errorf("unsupported int size %d", size)
}

func getRandomInt64LimiterForDynamicParameter(size int, requestedMinValue, requestedMaxValue int64) (*transformers.Int64Limiter, error) {
	minValue, maxValue, err := getIntThresholds(size)
	if err != nil {
		return nil, err
	}

	if !limitIsValid(requestedMinValue, minValue, maxValue) {
		return nil, fmt.Errorf("requested dynamic parameter min value is out of range of int%d size", size)
	}

	if !limitIsValid(requestedMaxValue, minValue, maxValue) {
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

func limitIsValid[T int64 | float64](requestedThreshold, minValue, maxValue T) bool {
	return requestedThreshold >= minValue || requestedThreshold <= maxValue
}

func validateIntTypeAndSetRandomInt64Limiter(
	size int, requestedMinValue, requestedMaxValue *int64,
) (limiter *transformers.Int64Limiter, warns toolkit.ValidationWarnings, err error) {

	minValue, maxValue, warns, err := validateInt64AndGetLimits(size, requestedMinValue, requestedMaxValue)
	if err != nil {
		return nil, nil, err
	}
	if warns.IsFatal() {
		return nil, warns, nil
	}
	l, err := transformers.NewInt64Limiter(minValue, maxValue)
	if err != nil {
		return nil, nil, err
	}
	return l, nil, nil
}

func validateInt64AndGetLimits(
	size int, requestedMinValue, requestedMaxValue *int64,
) (int64, int64, toolkit.ValidationWarnings, error) {
	var warns toolkit.ValidationWarnings
	minValue, maxValue, err := getIntThresholds(size)
	if err != nil {
		return 0, 0, nil, err
	}
	if requestedMinValue == nil {
		requestedMinValue = &minValue
	}
	if requestedMaxValue == nil {
		requestedMaxValue = &maxValue
	}

	if !limitIsValid(*requestedMinValue, minValue, maxValue) {
		warns = append(warns, toolkit.NewValidationWarning().
			SetMsgf("requested min value is out of int%d range", size).
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("AllowedMinValue", minValue).
			AddMeta("AllowedMaxValue", maxValue).
			AddMeta("ParameterName", "min").
			AddMeta("ParameterValue", requestedMinValue),
		)
	}

	if !limitIsValid(*requestedMaxValue, minValue, maxValue) {
		warns = append(warns, toolkit.NewValidationWarning().
			SetMsgf("requested max value is out of int%d range", size).
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("AllowedMinValue", minValue).
			AddMeta("AllowedMaxValue", maxValue).
			AddMeta("ParameterName", "min").
			AddMeta("ParameterValue", requestedMinValue),
		)
	}

	if warns.IsFatal() {
		return 0, 0, warns, nil
	}

	return *requestedMinValue, *requestedMaxValue, nil, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(integerTransformerDefinition)
}
