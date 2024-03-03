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
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	Int2Length = 2
	Int4Length = 4
	Int8Length = 8
)

var commonIntegerTransformerParams = []*toolkit.Parameter{
	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("int2", "int4", "int8", "numeric"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"min",
		"min int value threshold. By default the minimum possible value for the type",
	).SetRequired(false),

	toolkit.MustNewParameter(
		"max",
		"max int value threshold. By default the maximum possible value for the type",
	).SetRequired(false),

	toolkit.MustNewParameter(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(toolkit.ParamsValue("true")),
}

var RandomIntegerTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"random.Integer",
		"Generate integer value in min and max thresholds",
	),

	NewRandomIntTransformer,

	commonIntegerTransformerParams...,
)

type RandomIntTransformer struct {
	columnName      string
	keepNull        bool
	min             int64
	max             int64
	rand            *rand.Rand
	affectedColumns map[int]string
	columnIdx       int
}

func NewDeterministicIntTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (utils.Transformer, toolkit.ValidationWarnings, error) {
	gen, err := getGeneratorWithProjectedOutput(Sha1HashFunction, 8)
	if err != nil {
		return nil, nil, err
	}
	return NewIntTransformer(ctx, driver, parameters, gen)
}

func NewRandomIntTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (utils.Transformer, toolkit.ValidationWarnings, error) {
	seed := time.Now().UnixNano()
	return NewIntTransformer(ctx, driver, parameters, generators.NewInt64Random(seed))
}

func NewIntTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter, g generators.Generator) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string
	var minVal, maxVal int64
	var keepNull bool

	p := parameters["column"]
	if _, err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, c, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["min"]
	if _, err := p.Scan(&minVal); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	p = parameters["max"]
	if _, err := p.Scan(&maxVal); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	if minVal >= maxVal {
		return nil, toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				AddMeta("min", minVal).
				AddMeta("max", maxVal).
				SetMsg("max value must be greater that min value"),
		}, nil
	}

	limiter, limitsWarnings, err := validateIntTypeAndSetLimit(c, minVal, maxVal)
	if err != nil {
		return nil, nil, err
	}
	if limitsWarnings.IsFatal() {
		return nil, limitsWarnings, nil
	}

	p = parameters["keep_null"]
	if _, err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &RandomIntTransformer{
		columnName:      columnName,
		keepNull:        keepNull,
		min:             minVal,
		max:             maxVal,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		affectedColumns: affectedColumns,
		columnIdx:       idx,
	}, nil, nil
}

func (rit *RandomIntTransformer) GetAffectedColumns() map[int]string {
	return rit.affectedColumns
}

func (rit *RandomIntTransformer) Init(ctx context.Context) error {
	return nil
}

func (rit *RandomIntTransformer) Done(ctx context.Context) error {
	return nil
}

func (rit *RandomIntTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(rit.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rit.keepNull {
		return r, nil
	}

	if err := r.SetColumnValueByIdx(rit.columnIdx, utils.RandomInt(rit.rand, rit.min, rit.max)); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func validateIntTypeAndSetLimit(
	c *toolkit.Column, requestedMinValue, requestedMaxValue int64,
) (limiter *transformers.Int64Limiter, warns toolkit.ValidationWarnings, err error) {

	switch c.Length {
	case Int2Length:
		if requestedMinValue < math.MinInt16 || requestedMinValue > math.MinInt16 {
			warns = append(warns, toolkit.NewValidationWarning().
				SetMsg("requested min value is out of int16 range").
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("AllowedMinValue", math.MinInt16).
				AddMeta("AllowedMaxValue", math.MaxInt16).
				AddMeta("ParameterName", "min").
				AddMeta("ParameterValue", requestedMinValue),
			)
		}
		if requestedMaxValue < math.MinInt16 || requestedMaxValue > math.MinInt16 {
			warns = append(warns, toolkit.NewValidationWarning().
				SetMsg("requested max value is out of int16 range").
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("AllowedMinValue", math.MinInt16).
				AddMeta("AllowedMaxValue", math.MaxInt16).
				AddMeta("ParameterName", "max").
				AddMeta("ParameterValue", requestedMinValue),
			)
		}

		limiter, err = transformers.NewInt64Limiter(math.MinInt16, math.MaxInt16)
		if err != nil {
			return nil, nil, err
		}
	case Int4Length:
		if requestedMinValue < math.MinInt32 || requestedMinValue > math.MinInt32 {
			warns = append(warns, toolkit.NewValidationWarning().
				SetMsg("requested min value is out of int32 range").
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("AllowedMinValue", math.MinInt32).
				AddMeta("AllowedMaxValue", math.MaxInt32).
				AddMeta("ParameterName", "min").
				AddMeta("ParameterValue", requestedMinValue),
			)
		}
		if requestedMaxValue < math.MinInt32 || requestedMaxValue > math.MinInt32 {
			warns = append(warns, toolkit.NewValidationWarning().
				SetMsg("requested max value is out of int32 range").
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("AllowedMinValue", math.MinInt32).
				AddMeta("AllowedMaxValue", math.MaxInt32).
				AddMeta("ParameterName", "max").
				AddMeta("ParameterValue", requestedMinValue),
			)
		}
		limiter, err = transformers.NewInt64Limiter(math.MinInt32, math.MaxInt32)
		if err != nil {
			return nil, nil, err
		}
	case Int8Length:
		if requestedMinValue < math.MinInt64 || requestedMinValue > math.MinInt64 {
			warns = append(warns, toolkit.NewValidationWarning().
				SetMsg("requested min value is out of int64 range").
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("AllowedMinValue", math.MinInt64).
				AddMeta("AllowedMaxValue", math.MaxInt64).
				AddMeta("ParameterName", "min").
				AddMeta("ParameterValue", requestedMinValue),
			)
		}
		if requestedMaxValue < math.MinInt64 || requestedMaxValue > math.MinInt64 {
			warns = append(warns, toolkit.NewValidationWarning().
				SetMsg("requested max value is out of int64 range").
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("AllowedMinValue", math.MinInt64).
				AddMeta("AllowedMaxValue", math.MaxInt64).
				AddMeta("ParameterName", "max").
				AddMeta("ParameterValue", requestedMinValue),
			)
		}

		limiter, err = transformers.NewInt64Limiter(math.MinInt64, math.MaxInt64)
		if err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, errors.New("FIXME: seems that type length was gathered incorrectly")
	}

	if warns.IsFatal() {
		return nil, warns, nil
	}

	if requestedMinValue != 0 || requestedMaxValue != 0 {
		limiter, err = transformers.NewInt64Limiter(requestedMinValue, requestedMaxValue)
		if err != nil {
			return nil, nil, err
		}
	}

	return limiter, nil, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomIntegerTransformerDefinition)
}
