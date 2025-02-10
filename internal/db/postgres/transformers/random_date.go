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
	"slices"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const RandomDateTransformerName = "RandomDate"

var truncateParts = []string{
	transformers.YearTruncateName, transformers.MonthTruncateName, transformers.DayTruncateName,
	transformers.HourTruncateName, transformers.SecondTruncateName, transformers.MillisecondTruncateName,
	transformers.MicrosecondTruncateName, transformers.NanosecondTruncateName,
}

var timestampTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		RandomDateTransformerName,
		"Generate date in the provided interval",
	).AddMeta(AllowApplyForReferenced, true).
		AddMeta(RequireHashEngineParameter, true),

	NewTimestampTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("date", "timestamp", "timestamptz"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"min",
		"min threshold date (and/or time) of value",
	).SetRequired(true).
		SetSupportTemplate(true).
		SetLinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("date", "timestamp", "timestamptz"),
		),

	toolkit.MustNewParameterDefinition(
		"max",
		"max threshold date (and/or time) of value",
	).SetRequired(true).
		SetSupportTemplate(true).
		SetLinkParameter("column").
		SetSupportTemplate(true).
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("date", "timestamp", "timestamptz"),
		),

	truncateDateParameterDefinition,

	keepNullParameterDefinition,

	engineParameterDefinition,
)

type TimestampTransformer struct {
	*transformers.Timestamp
	columnName      string
	columnIdx       int
	keepNull        bool
	affectedColumns map[int]string

	columnParam   toolkit.Parameterizer
	maxParam      toolkit.Parameterizer
	minParam      toolkit.Parameterizer
	truncateParam toolkit.Parameterizer
	keepNullParam toolkit.Parameterizer
	engineParam   toolkit.Parameterizer
	dynamicMode   bool

	transform func([]byte) (time.Time, error)
}

type timestampMinMaxEncoder func(toolkit.Parameterizer, toolkit.Parameterizer) (time.Time, time.Time, error)

func NewTimestampTransformerBase(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer, encoder timestampMinMaxEncoder) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var dynamicMode bool

	columnParam := parameters["column"]
	maxParam := parameters["max"]
	minParam := parameters["min"]
	truncateParam := parameters["truncate"]
	keepNullParam := parameters["keep_null"]
	engineParam := parameters["engine"]

	var columnName, truncate, engine string
	var keepNull bool

	if err := engineParam.Scan(&engine); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "engine" param: %w`, err)
	}

	if minParam.IsDynamic() || maxParam.IsDynamic() {
		dynamicMode = true
	}

	if err := columnParam.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	if err := keepNullParam.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	if err := truncateParam.Scan(&truncate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "truncate" param: %w`, err)
	}

	var minVal, maxVal time.Time
	var limiter *transformers.TimestampLimiter
	var err error
	if !dynamicMode {
		minVal, maxVal, err = encoder(minParam, maxParam)

		if err != nil {
			return nil, nil, fmt.Errorf("error getting min and max values: %w", err)
		}
		limiter, err = transformers.NewTimestampLimiter(minVal, maxVal)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create timestamp limiter: %w", err)
		}
	}

	t, err := transformers.NewRandomTimestamp(truncate, limiter)
	if err != nil {
		return nil, nil, err
	}

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}

	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &TimestampTransformer{
		Timestamp:       t,
		keepNull:        keepNull,
		columnName:      columnName,
		columnIdx:       idx,
		affectedColumns: affectedColumns,

		columnParam:   columnParam,
		minParam:      minParam,
		maxParam:      maxParam,
		truncateParam: truncateParam,
		keepNullParam: keepNullParam,
		engineParam:   engineParam,
		dynamicMode:   dynamicMode,
		transform: func(bytes []byte) (time.Time, error) {
			return t.Transform(nil, bytes)
		},
	}, nil, nil
}

func NewTimestampTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	return NewTimestampTransformerBase(ctx, driver, parameters, getTimestampMinAndMaxThresholds)
}

func (rdt *TimestampTransformer) GetAffectedColumns() map[int]string {
	return rdt.affectedColumns
}

func (rdt *TimestampTransformer) Init(ctx context.Context) error {
	if rdt.dynamicMode {
		rdt.transform = rdt.dynamicTransform
	}
	return nil
}

func (rdt *TimestampTransformer) Done(ctx context.Context) error {
	return nil
}

func (rdt *TimestampTransformer) dynamicTransform(v []byte) (time.Time, error) {
	var minVal, maxVal time.Time
	err := rdt.minParam.Scan(&minVal)
	if err != nil {
		return time.Time{}, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	err = rdt.maxParam.Scan(&maxVal)
	if err != nil {
		return time.Time{}, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	limiter, err := transformers.NewTimestampLimiter(minVal, maxVal)
	if err != nil {
		return time.Time{}, fmt.Errorf("error creating limiter in dynamic mode: %w", err)
	}
	res, err := rdt.Timestamp.Transform(limiter, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("error generating timestamp value: %w", err)
	}
	return res, nil
}

func (rdt *TimestampTransformer) Transform(_ context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	valAny, err := r.GetRawColumnValueByIdx(rdt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rdt.keepNull {
		return r, nil
	}
	res, err := rdt.transform(valAny.Data)
	if err != nil {
		return nil, err
	}
	if err = r.SetColumnValueByIdx(rdt.columnIdx, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func validateDateTruncationParameterValue(p *toolkit.ParameterDefinition, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {

	if !slices.Contains(truncateParts, string(v)) && string(v) != "" {
		return toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("ParameterValue", string(v)).
				AddMeta("AllowedValues", truncateParts).
				SetMsg("wrong truncation part value"),
		}, nil
	}
	return nil, nil
}

func getTimestampMinAndMaxThresholds(minParameter, maxParameter toolkit.Parameterizer) (time.Time, time.Time, error) {
	var minVal, maxVal time.Time
	if err := minParameter.Scan(&minVal); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("error scanning \"min\" parameter: %w", err)
	}
	if err := maxParameter.Scan(&maxVal); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("error scanning \"max\" parameter: %w", err)
	}
	return minVal, maxVal, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(timestampTransformerDefinition)
}
