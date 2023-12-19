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

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// TODO: Ensure pqinterval.Duration returns duration in int64 for date and time

var NoiseDateTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"NoiseDate",
		"Add some random value (shift value) in the provided interval",
	),

	NewNoiseDateTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("date", "timestamp", "timestamptz").
		SetSkipOnNull(true),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"ratio",
		"max random duration for noise",
	).SetRequired(true).
		SetCastDbType("interval"),

	toolkit.MustNewParameter(
		"truncate",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
	).SetRawValueValidator(ValidateDateTruncationParameterValue),
)

type dateNoiseFunc func(r *rand.Rand, ratio time.Duration, original *time.Time, truncate *string) *time.Time

type NoiseDateTransformer struct {
	columnName      string
	columnIdx       int
	ratioVal        any
	truncate        *string
	rand            *rand.Rand
	generate        dateNoiseFunc
	affectedColumns map[int]string
	res             *time.Time
	interval        *pgtype.Interval
	ratio           time.Duration
}

func NewNoiseDateTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName, truncate string
	var generator dateNoiseFunc = generateNoisedTime

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

	p = parameters["ratio"]
	v, err := p.Value()
	if err != nil {
		return nil, nil, fmt.Errorf(`error parsing "ratio" parameter: %w`, err)
	}
	intervalValue, ok := v.(pgtype.Interval)
	if !ok {
		return nil, nil, fmt.Errorf(`cannot cast "ratio" param to interval value`)
	}

	if intervalValue.Months == 0 && intervalValue.Days == 0 && intervalValue.Microseconds == 0 {
		return nil,
			toolkit.ValidationWarnings{
				toolkit.NewValidationWarning().
					SetMsg(`error parsing ratio parameter or received 0 value`).
					AddMeta("ParameterName", "ratio").
					SetSeverity(toolkit.ErrorValidationSeverity),
			},
			nil
	}

	p = parameters["truncate"]
	if _, err := p.Scan(&truncate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "truncate" param: %w`, err)
	}

	if truncate != "" {
		generator = generateNoisedTimeTruncate
	}

	ratio := (time.Duration(intervalValue.Days) * time.Hour * 24) +
		(time.Duration(intervalValue.Months) * 30 * time.Hour * 24) +
		(time.Duration(intervalValue.Microseconds) * time.Millisecond)

	return &NoiseDateTransformer{
		columnName:      columnName,
		ratioVal:        intervalValue,
		truncate:        &truncate,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		generate:        generator,
		affectedColumns: affectedColumns,
		res:             new(time.Time),
		columnIdx:       idx,
		interval:        &intervalValue,
		ratio:           ratio,
	}, nil, nil
}

func (ndt *NoiseDateTransformer) GetAffectedColumns() map[int]string {
	return ndt.affectedColumns
}

func (ndt *NoiseDateTransformer) Init(ctx context.Context) error {
	return nil
}

func (ndt *NoiseDateTransformer) Done(ctx context.Context) error {
	return nil
}

func (ndt *NoiseDateTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	isNull, err := r.ScanColumnValueByIdx(ndt.columnIdx, ndt.res)
	if err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if isNull {
		return r, nil
	}

	resTime := ndt.generate(ndt.rand, ndt.ratio, ndt.res, ndt.truncate)
	if err := r.SetColumnValueByIdx(ndt.columnIdx, resTime); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func generateNoisedTime(r *rand.Rand, ratio time.Duration, val *time.Time, truncate *string) *time.Time {
	return utils.NoiseDateV2(r, ratio, val)
}

func generateNoisedTimeTruncate(r *rand.Rand, ratio time.Duration, val *time.Time, truncate *string) *time.Time {
	res, _ := utils.TruncateDate(truncate, utils.NoiseDateV2(r, ratio, val))
	return res
}

func ValidateDateTruncationParameterValue(p *toolkit.Parameter, v toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
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
	utils.DefaultTransformerRegistry.MustRegister(NoiseDateTransformerDefinition)
}
