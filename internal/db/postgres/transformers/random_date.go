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
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var truncateParts = []string{"year", "month", "day", "hour", "second", "millisecond", "microsecond", "nanosecond"}

var RandomDateTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"RandomDate",
		"Generate random date in the provided interval",
	),

	NewRandomDateTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("date", "timestamp", "timestamptz"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"min",
		"min threshold date (and/or time) of random value",
	).SetRequired(true).
		SetLinkParameter("column"),

	toolkit.MustNewParameter(
		"max",
		"max threshold date  (and/or time) of random value",
	).SetRequired(true).
		SetLinkParameter("column"),

	toolkit.MustNewParameter(
		"truncate",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
	),

	toolkit.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
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
	columnName      string
	columnIdx       int
	rand            *rand.Rand
	generate        dateGeneratorFunc
	min             *time.Time
	max             *time.Time
	truncate        string
	keepNull        bool
	delta           *int64
	affectedColumns map[int]string
}

func NewRandomDateTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName, truncate string
	var minTime, maxTime time.Time
	var generator dateGeneratorFunc = generateRandomTime
	var keepNull bool

	p := parameters["column"]
	if _, err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["min"]
	v, err := p.Value()
	if err != nil {
		return nil, nil, fmt.Errorf(`error parsing "min" parameter: %w`, err)
	}
	minTime, ok = v.(time.Time)
	if !ok {
		return nil, nil, errors.New(`unexpected type for "min" parameter`)
	}

	p = parameters["max"]
	v, err = p.Value()
	if err != nil {
		return nil, nil, fmt.Errorf(`error parsing "max" parameter: %w`, err)
	}

	maxTime, ok = v.(time.Time)
	if !ok {
		return nil, nil, errors.New(`unexpected type for "max" parameter`)
	}

	p = parameters["keep_null"]
	if _, err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	p = parameters["truncate"]
	if _, err := p.Scan(&truncate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "truncate" param: %w`, err)
	}

	if truncate != "" {
		generator = generateRandomTimeTruncate
	}

	if minTime.After(maxTime) {
		return nil, toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				AddMeta("max", maxTime).
				AddMeta("min", minTime).
				SetMsg("max value must be greater than min"),
		}, nil
	}
	delta := int64(maxTime.Sub(minTime))
	return &RandomDateTransformer{
		keepNull:        keepNull,
		truncate:        truncate,
		columnName:      columnName,
		columnIdx:       idx,
		min:             &minTime,
		max:             &maxTime,
		generate:        generator,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		affectedColumns: affectedColumns,
		delta:           &delta,
	}, nil, nil

}

func (rdt *RandomDateTransformer) GetAffectedColumns() map[int]string {
	return rdt.affectedColumns
}

func (rdt *RandomDateTransformer) Init(ctx context.Context) error {
	return nil
}

func (rdt *RandomDateTransformer) Done(ctx context.Context) error {
	return nil
}

func (rdt *RandomDateTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	valAny, err := r.GetRawAttributeValueByIdx(rdt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rdt.keepNull {
		return r, nil
	}

	res := rdt.generate(rdt.rand, rdt.min, rdt.delta, &rdt.truncate)
	if err := r.SetAttributeValueByIdx(rdt.columnIdx, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func generateRandomTime(r *rand.Rand, startDate *time.Time, delta *int64, truncate *string) *time.Time {
	res := startDate.Add(time.Duration(r.Int63n(*delta)))
	return &res
}

func generateRandomTimeTruncate(r *rand.Rand, startDate *time.Time, delta *int64, truncate *string) *time.Time {
	res, _ := utils.TruncateDate(truncate, generateRandomTime(r, startDate, delta, truncate))
	return res
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomDateTransformerDefinition)
}
