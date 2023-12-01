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
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var RandomStringTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"RandomString",
		"Generate random string",
	),

	NewRandomStringTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"min_length",
		"min length of string",
	).SetRequired(true),

	toolkit.MustNewParameter(
		"max_length",
		"max length of string",
	).SetRequired(true),

	toolkit.MustNewParameter(
		"symbols",
		"the characters range for random string",
	).SetDefaultValue(toolkit.ParamsValue("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")),

	toolkit.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
	).SetDefaultValue(toolkit.ParamsValue("true")),
)

type RandomStringTransformer struct {
	columnName      string
	keepNull        bool
	min             int64
	max             int64
	symbols         []rune
	buf             []rune
	rand            *rand.Rand
	affectedColumns map[int]string
	columnIdx       int
}

func NewRandomStringTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName, symbols string
	var minLength, maxLength int64
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

	p = parameters["min_length"]
	if _, err := p.Scan(&minLength); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "min_length" param: %w`, err)
	}

	p = parameters["max_length"]
	if _, err := p.Scan(&maxLength); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "max_length" param: %w`, err)
	}

	p = parameters["symbols"]
	if _, err := p.Scan(&symbols); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "symbols" param: %w`, err)
	}

	p = parameters["keep_null"]
	if _, err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &RandomStringTransformer{
		columnName:      columnName,
		keepNull:        keepNull,
		min:             minLength,
		max:             maxLength,
		symbols:         []rune(symbols),
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		buf:             make([]rune, maxLength),
		affectedColumns: affectedColumns,
		columnIdx:       idx,
	}, nil, nil
}

func (rst *RandomStringTransformer) GetAffectedColumns() map[int]string {
	return rst.affectedColumns
}

func (rst *RandomStringTransformer) Init(ctx context.Context) error {
	return nil
}

func (rst *RandomStringTransformer) Done(ctx context.Context) error {
	return nil
}

func (rst *RandomStringTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawAttributeValueByIdx(rst.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rst.keepNull {
		return r, nil
	}

	res := utils.RandomString(rst.rand, rst.min, rst.max, rst.symbols, rst.buf)
	if err := r.SetAttributeValueByIdx(rst.columnIdx, &res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}
