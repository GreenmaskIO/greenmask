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

var RandomBoolTransformerDefinition = utils.NewDefinition(

	utils.NewTransformerProperties(
		"RandomBool",
		"Generate random bool",
	),

	NewRandomBoolTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("bool"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
	).SetDefaultValue(toolkit.ParamsValue("true")),
)

type RandomBoolTransformer struct {
	columnName      string
	keepNull        bool
	rand            *rand.Rand
	affectedColumns map[int]string
	columnIdx       int
}

func NewRandomBoolTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string
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

	p = parameters["keep_null"]
	if _, err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &RandomBoolTransformer{
		columnName:      columnName,
		keepNull:        keepNull,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		affectedColumns: affectedColumns,
		columnIdx:       idx,
	}, nil, nil
}

func (rbt *RandomBoolTransformer) GetAffectedColumns() map[int]string {
	return rbt.affectedColumns
}

func (rbt *RandomBoolTransformer) Init(ctx context.Context) error {
	return nil
}

func (rbt *RandomBoolTransformer) Done(ctx context.Context) error {
	return nil
}

func (rbt *RandomBoolTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawAttributeValueByIdx(rbt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rbt.keepNull {
		return r, nil
	}

	if err := r.SetAttributeValueByIdx(rbt.columnIdx, utils.RandomBool(rbt.rand)); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomBoolTransformerDefinition)
}
