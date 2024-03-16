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

package transformers_new

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var truncateParts = []string{"year", "month", "day", "hour", "second", "millisecond", "microsecond", "nanosecond"}

const (
	timestampTransformerName        = "Timestamp"
	timestampTransformerDescription = "Generate date in the provided interval"
)

var timestampTransformerParams = []*toolkit.ParameterDefinition{
	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("date", "timestamp", "timestamptz"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"min",
		"min threshold date (and/or time) of random value",
	).SetRequired(true).
		SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("date", "timestamp", "timestamptz"),
		),

	toolkit.MustNewParameterDefinition(
		"max",
		"max threshold date  (and/or time) of random value",
	).SetRequired(true).
		SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("date", "timestamp", "timestamptz"),
		),

	toolkit.MustNewParameterDefinition(
		"truncate",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
	).SetRawValueValidator(ValidateDateTruncationParameterValue),

	toolkit.MustNewParameterDefinition(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(toolkit.ParamsValue("true")),
}

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
	dynamicMode   bool

	transform func(context.Context, []byte) (time.Time, error)
}

func NewTimestampTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (UnifiedTransformer, toolkit.ValidationWarnings, error) {

	var dynamicMode bool

	columnParam := parameters["column"]
	maxParam := parameters["max"]
	minParam := parameters["min"]
	truncateParam := parameters["truncate"]
	keepNullParam := parameters["keep_null"]

	if minParam.IsDynamic() || maxParam.IsDynamic() {
		dynamicMode = true
	}

	var columnName, truncate string
	var keepNull bool

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
		if err := minParam.Scan(&minVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"min\" parameter: %w", err)
		}
		if err := maxParam.Scan(&maxVal); err != nil {
			return nil, nil, fmt.Errorf("error scanning \"max\" parameter: %w", err)
		}
		limiter, err = transformers.NewTimestampLimiter(minVal, maxVal)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create timestamp limiter: %w", err)
		}
	}

	t, err := transformers.NewTimestamp(truncate, limiter)
	if err != nil {
		return nil, nil, err
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
		dynamicMode:   dynamicMode,
		transform:     t.Transform,
	}, nil, nil

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

func (rdt *TimestampTransformer) dynamicTransform(ctx context.Context, v []byte) (time.Time, error) {
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
	ctx = context.WithValue(ctx, "limiter", limiter)
	res, err := rdt.Timestamp.Transform(ctx, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("error generating timestamp value: %w", err)
	}
	return res, nil
}

func (rdt *TimestampTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	valAny, err := r.GetRawColumnValueByIdx(rdt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rdt.keepNull {
		return r, nil
	}
	res, err := rdt.transform(ctx, valAny.Data)
	if err != nil {
		return nil, err
	}
	if err = r.SetColumnValueByIdx(rdt.columnIdx, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func ValidateDateTruncationParameterValue(p *toolkit.ParameterDefinition, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
	part := string(v)
	switch part {
	case "nano", "second", "minute", "hour", "day", "month", "year", "":
		return nil, nil
	default:
		return toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("ParameterValue", part).
				SetMsg("wrong truncation part value: must be one of nano, second, minute, hour, day, month, year"),
		}, nil
	}
}

func init() {

	registerRandomAndDeterministicTransformer(
		utils.DefaultTransformerRegistry,
		timestampTransformerName,
		timestampTransformerDescription,
		NewTimestampTransformer,
		timestampTransformerParams,
		transformers.TimestampTransformerByteLength,
	)
}
