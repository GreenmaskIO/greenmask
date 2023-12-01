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

var RandomFloatTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"RandomFloat",
		"Generate random float",
	),

	NewRandomFloatTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("float4", "float8"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"min",
		"min float threshold of random value. The value range depends on column type",
	).SetRequired(true),

	toolkit.MustNewParameter(
		"max",
		"min float threshold of random value. The value range depends on column type",
	).SetRequired(true),

	toolkit.MustNewParameter(
		"precision",
		"precision of random float value (number of digits after coma)",
	).SetDefaultValue(toolkit.ParamsValue("4")),

	toolkit.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
	).SetDefaultValue(toolkit.ParamsValue("true")),
)

type RandomFloatTransformer struct {
	columnName      string
	keepNull        bool
	min             float64
	max             float64
	precision       int
	rand            *rand.Rand
	affectedColumns map[int]string
	columnIdx       int
}

func NewRandomFloatTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string
	var minVal, maxVal float64
	var precision int
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
	if _, err := p.Scan(&minVal); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "min" param: %w`, err)
	}

	p = parameters["max"]
	if _, err := p.Scan(&maxVal); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "max" param: %w`, err)
	}

	p = parameters["precision"]
	if _, err := p.Scan(&precision); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "precision" param: %w`, err)
	}

	p = parameters["keep_null"]
	if _, err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &RandomFloatTransformer{
		keepNull:        keepNull,
		precision:       precision,
		min:             minVal,
		max:             maxVal,
		columnName:      columnName,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		affectedColumns: affectedColumns,
		columnIdx:       idx,
	}, nil, nil

}

func (rft *RandomFloatTransformer) GetAffectedColumns() map[int]string {
	return rft.affectedColumns
}

func (rft *RandomFloatTransformer) Init(ctx context.Context) error {
	return nil
}

func (rft *RandomFloatTransformer) Done(ctx context.Context) error {
	return nil
}

func (rft *RandomFloatTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	valAny, err := r.GetRawAttributeValueByIdx(rft.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rft.keepNull {
		return r, nil
	}

	err = r.SetAttributeValueByIdx(rft.columnIdx, utils.RandomFloat(rft.rand, rft.min, rft.max, rft.precision))
	if err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomFloatTransformerDefinition)
}
