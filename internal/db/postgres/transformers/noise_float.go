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

var NoiseFloatTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"NoiseFloat",
		"Add noise to float value in min and max thresholds",
	),
	NewNoiseFloatTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("float4", "float8").
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
				SetCompatibleTypes("float4", "float8", "int2", "int4", "int8"),
		),

	toolkit.MustNewParameterDefinition(
		"max",
		"max float value threshold",
	).SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("float4", "float8", "int2", "int4", "int8"),
		),

	minRatioParameterDefinition,

	maxRatioParameterDefinition,

	engineParameterDefinition,
)

type NoiseFloatTransformer struct {
	t               *transformers.NoiseFloat64Transformer
	columnName      string
	columnIdx       int
	precision       int
	affectedColumns map[int]string
	dynamicMode     bool
	floatSize       int

	columnParam    toolkit.Parameterizer
	maxParam       toolkit.Parameterizer
	minParam       toolkit.Parameterizer
	engineParam    toolkit.Parameterizer
	precisionParam toolkit.Parameterizer
	maxRatioParam  toolkit.Parameterizer
	minRatioParam  toolkit.Parameterizer

	transform func(context.Context, float64) (float64, error)
}

func NewNoiseFloatTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName, engine string
	var dynamicMode bool
	var minValueThreshold, maxValueThreshold, minRatio, maxRatio float64
	var precision int

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
	floatSize := c.GetColumnSize()

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

	limiter, limitsWarnings, err := validateNoiseFloatTypeAndSetLimit(floatSize, minValueThreshold, maxValueThreshold, precision)
	if err != nil {
		return nil, nil, err
	}
	if limitsWarnings.IsFatal() {
		return nil, limitsWarnings, nil
	}

	t := transformers.NewNoiseFloat64Transformer(limiter, minRatio, maxRatio)

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &NoiseFloatTransformer{
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

		dynamicMode: dynamicMode,
		floatSize:   floatSize,

		transform:     t.Transform,
		maxRatioParam: maxRatioParam,
		minRatioParam: minRatioParam,
	}, nil, nil
}

func (nft *NoiseFloatTransformer) GetAffectedColumns() map[int]string {
	return nft.affectedColumns
}

func (nft *NoiseFloatTransformer) Init(ctx context.Context) error {
	if nft.dynamicMode {
		nft.transform = nft.dynamicTransform
	}
	return nil
}

func (nft *NoiseFloatTransformer) Done(ctx context.Context) error {
	return nil
}

func (nft *NoiseFloatTransformer) dynamicTransform(ctx context.Context, v float64) (float64, error) {
	minVal, maxVal, err := getMinAndMaxFloatDynamicValueNoiseIntTrans(nft.floatSize, nft.minParam, nft.maxParam)
	if err != nil {
		return 0, fmt.Errorf("unable to get min and max values: %w", err)
	}

	limiter, err := transformers.NewNoiseFloat64Limiter(minVal, maxVal, nft.precision)
	if err != nil {
		return 0, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	ctx = context.WithValue(ctx, "limiter", limiter)
	res, err := nft.t.Transform(ctx, v)
	if err != nil {
		return 0, fmt.Errorf("error generating int value: %w", err)
	}
	return res, nil
}

func (nft *NoiseFloatTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var val float64
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

func validateNoiseFloatTypeAndSetLimit(
	size int, requestedMinValue, requestedMaxValue float64, precision int,
) (limiter *transformers.NoiseFloat64Limiter, warns toolkit.ValidationWarnings, err error) {

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

	limiter, err = transformers.NewNoiseFloat64Limiter(-math.MaxFloat64, math.MaxFloat64, precision)
	if err != nil {
		return nil, nil, err
	}

	if requestedMinValue != 0 || requestedMaxValue != 0 {
		limiter, err = transformers.NewNoiseFloat64Limiter(requestedMinValue, requestedMaxValue, precision)
		if err != nil {
			return nil, nil, err
		}
	}

	return limiter, nil, nil
}

func getMinAndMaxFloatDynamicValueNoiseIntTrans(floatSize int, minParam, maxParam toolkit.Parameterizer) (float64, float64, error) {

	var requestedMinValue, requestedMaxValue float64
	var minRequested, maxRequested bool
	minValue, maxValue, err := getFloatThresholds(floatSize)
	if err != nil {
		return 0, 0, err
	}

	if minParam.IsDynamic() {
		minRequested = true
		err = minParam.Scan(&requestedMinValue)
		if err != nil {
			return 0, 0, fmt.Errorf(`unable to scan "min" dynamic  param: %w`, err)
		}
		if !limitIsValid(requestedMinValue, minValue, maxValue) {
			return 0, 0, fmt.Errorf("requested dynamic parameter min value is out of range of float%d size", floatSize)
		}
	}

	if maxParam.IsDynamic() {
		maxRequested = true
		err = minParam.Scan(&maxValue)
		if err != nil {
			return 0, 0, fmt.Errorf(`unable to scan "max" dynamic param: %w`, err)
		}
		if !limitIsValid(requestedMaxValue, minValue, maxValue) {
			return 0, 0, fmt.Errorf("requested dynamic parameter max value is out of range of float%d size", floatSize)
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

func init() {
	utils.DefaultTransformerRegistry.MustRegister(NoiseFloatTransformerDefinition)
}
