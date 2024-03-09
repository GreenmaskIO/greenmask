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
	"math/rand"
	"strings"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var truncateParts = []string{"year", "month", "day", "hour", "second", "millisecond", "microsecond", "nanosecond"}

var RandomDateTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"RandomDate",
		"Generate random date in the provided interval",
	),

	NewRandomDateTransformer,

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
)

type dateGeneratorFunc func(r *rand.Rand, startDate *time.Time, delta *int64, truncate *string) *time.Time

type RandomDateTransformerParams struct {
	Min      string  `mapstructure:"min" validate:"required"`
	Max      string  `mapstructure:"max" validate:"required"`
	Truncate string  `mapstructure:"truncate" validate:"omitempty,oneof=year month day hour second nano"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RandomDateTransformer struct {
	*transformers.Timestamp
	columnName      string
	columnIdx       int
	rand            *rand.Rand
	generate        dateGeneratorFunc
	truncate        string
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

func NewRandomDateTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var dynamicMode bool

	columnParam := parameters["column"]
	maxParam := parameters["max"]
	minParam := parameters["min"]
	truncateParam := parameters["truncate"]
	keepNullParam := parameters["keep_null"]

	var columnName, truncate string
	var generator dateGeneratorFunc = generateRandomTime
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

	t, err := transformers.NewTimestamp(truncate, nil)
	if err != nil {
		return nil, nil, err
	}

	return &RandomDateTransformer{
		Timestamp:       t,
		keepNull:        keepNull,
		truncate:        truncate,
		columnName:      columnName,
		columnIdx:       idx,
		generate:        generator,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
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

func (rdt *RandomDateTransformer) GetAffectedColumns() map[int]string {
	return rdt.affectedColumns
}

func (rdt *RandomDateTransformer) Init(ctx context.Context) error {
	if rdt.dynamicMode {
		rdt.transform = rdt.dynamicTransform
	}
	return nil
}

func (rdt *RandomDateTransformer) Done(ctx context.Context) error {
	return nil
}

func (rdt *RandomDateTransformer) dynamicTransform(ctx context.Context, v []byte) (time.Time, error) {

}

func (rdt *RandomDateTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
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

func (rdt *RandomDateTransformer) oldTransform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	valAny, err := r.GetRawColumnValueByIdx(rdt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rdt.keepNull {
		return r, nil
	}

	minTime := &time.Time{}
	err = rdt.minParam.Scan(minTime)
	if err != nil {
		return nil, fmt.Errorf(`error getting "min" parameter value: %w`, err)
	}

	maxTime := &time.Time{}
	err = rdt.maxParam.Scan(maxTime)
	if err != nil {
		return nil, fmt.Errorf(`error getting "max" parameter value: %w`, err)
	}

	if minTime.After(*maxTime) {
		return nil, fmt.Errorf("max value must be greater than min: got min = %s max = %s", minTime.String(), maxTime.String())
	}

	delta := int64(maxTime.Sub(*minTime))

	res := rdt.generate(rdt.rand, minTime, &delta, &rdt.truncate)
	if err := r.SetColumnValueByIdx(rdt.columnIdx, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func generateRandomTime(r *rand.Rand, startDate *time.Time, delta *int64, truncate *string) *time.Time {
	res := startDate.Add(time.Duration(r.Int63n(*delta)))
	return &res
}

func generateRandomTimeTruncate(r *rand.Rand, startDate *time.Time, delta *int64, truncate *string) *time.Time {
	res, _ := toolkit.TruncateDate(truncate, generateRandomTime(r, startDate, delta, truncate))
	return res
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomDateTransformerDefinition)
}
