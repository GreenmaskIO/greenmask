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

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/shopspring/decimal"
)

var NoiseNumericTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"NoiseNumeric",
		"Add noise to numeric value in min and max thresholds",
	),
	NewNumericFloatTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("numeric", "decimal").
		SetSkipOnNull(true),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"precision",
		"precision of noised float value (number of digits after coma)",
	).SetDefaultValue(toolkit.ParamsValue("4")),

	toolkit.MustNewParameterDefinition(
		"min",
		"min float value threshold",
	).SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes(
					"float4",
					"float8",
					"numeric",
					"decimal",
					"int2",
					"int4",
					"int8",
				).SetUnmarshaler(numericTypeUnmarshaler),
		),

	toolkit.MustNewParameterDefinition(
		"max",
		"max float value threshold",
	).SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes(
					"float4",
					"float8",
					"numeric",
					"decimal",
					"int2",
					"int4",
					"int8",
				).SetUnmarshaler(numericTypeUnmarshaler),
		),

	minRatioParameterDefinition,

	maxRatioParameterDefinition,

	engineParameterDefinition,
)

// TODO: Add numeric introspection (getting the Numering settings)
type NoiseNumericTransformer struct {
	t               *transformers.NoiseNumericTransformer
	columnName      string
	columnIdx       int
	precision       int32
	affectedColumns map[int]string
	dynamicMode     bool

	minAllowedValue decimal.Decimal
	maxAllowedValue decimal.Decimal
	numericSize     int

	columnParam    toolkit.Parameterizer
	maxParam       toolkit.Parameterizer
	minParam       toolkit.Parameterizer
	engineParam    toolkit.Parameterizer
	precisionParam toolkit.Parameterizer
	maxRatioParam  toolkit.Parameterizer
	minRatioParam  toolkit.Parameterizer

	transform func(context.Context, decimal.Decimal) (decimal.Decimal, error)
}

func NewNumericFloatTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName, engine string
	var dynamicMode bool
	var minRatio, maxRatio float64
	var minValueThreshold, maxValueThreshold decimal.Decimal
	var precision int32

	columnParam := parameters["column"]
	minParam := parameters["min"]
	maxParam := parameters["max"]
	maxRatioParam := parameters["max_ratio"]
	minRatioParam := parameters["min_ratio"]
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

	if !dynamicMode {
		if err := minParam.Scan(&minValueThreshold); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"min\" parameter: %w", err)
		}
		if err := maxParam.Scan(&maxValueThreshold); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"max\" parameter: %w", err)
		}
	}

	if err := precisionParam.Scan(&precision); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "precision" param: %w`, err)
	}

	if err := minRatioParam.Scan(&minRatio); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"min_ratio\" param: %w", err)
	}

	if err := maxRatioParam.Scan(&maxRatio); err != nil {
		return nil, nil, fmt.Errorf("unable to scan \"max_ratio\" param: %w", err)
	}

	limiter, limitsWarnings, err := validateNoiseNumericTypeAndSetLimit(bigIntegerTransformerGenByteLength, minValueThreshold, maxValueThreshold)
	if err != nil {
		return nil, nil, err
	}
	if limitsWarnings.IsFatal() {
		return nil, limitsWarnings, nil
	}
	limiter.SetPrecision(precision)

	t := transformers.NewNoiseNumericTransformer(limiter, minRatio, maxRatio)

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &NoiseNumericTransformer{
		t:               t,
		columnName:      columnName,
		affectedColumns: affectedColumns,
		columnIdx:       idx,
		precision:       precision,

		columnParam:    columnParam,
		minParam:       minParam,
		maxParam:       maxParam,
		engineParam:    engineParam,
		precisionParam: precisionParam,

		minAllowedValue: limiter.MinValue,
		maxAllowedValue: limiter.MaxValue,
		numericSize:     c.Length,
		dynamicMode:     dynamicMode,

		transform:     t.Transform,
		maxRatioParam: maxRatioParam,
		minRatioParam: minRatioParam,
	}, nil, nil
}

func (nft *NoiseNumericTransformer) GetAffectedColumns() map[int]string {
	return nft.affectedColumns
}

func (nft *NoiseNumericTransformer) Init(ctx context.Context) error {
	if nft.dynamicMode {
		nft.transform = nft.dynamicTransform
	}
	return nil
}

func (nft *NoiseNumericTransformer) Done(ctx context.Context) error {
	return nil
}

func (nft *NoiseNumericTransformer) dynamicTransform(ctx context.Context, original decimal.Decimal) (decimal.Decimal, error) {
	var minVal, maxVal decimal.Decimal
	err := nft.minParam.Scan(&minVal)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	err = nft.maxParam.Scan(&maxVal)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	limiter, err := getNumericLimiterForDynamicParameter(nft.numericSize, minVal, maxVal, nft.minAllowedValue, nft.maxAllowedValue)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	ctx = context.WithValue(ctx, "limiter", limiter)
	return nft.t.Transform(ctx, original)
}

func (nft *NoiseNumericTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var val decimal.Decimal
	isNull, err := r.ScanColumnValueByIdx(nft.columnIdx, &val)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if isNull {
		return r, nil
	}

	res, err := nft.transform(ctx, val)
	if err != nil {
		return nil, fmt.Errorf("unable to transform value: %w", err)
	}

	if err = r.SetColumnValueByIdx(nft.columnIdx, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func validateNoiseNumericTypeAndSetLimit(
	size int, requestedMinValue, requestedMaxValue decimal.Decimal,
) (limiter *transformers.NoiseNumericLimiter, warns toolkit.ValidationWarnings, err error) {

	minVal, maxVal, warns, err := getNumericThresholds(size, requestedMinValue, requestedMaxValue)
	if err != nil {
		return nil, nil, err
	}
	if warns.IsFatal() {
		return nil, warns, nil
	}

	limiter, err = transformers.NewNoiseNumericLimiter(minVal, maxVal)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating limiter by size: %w", err)
	}

	return limiter, nil, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(NoiseNumericTransformerDefinition)
}
