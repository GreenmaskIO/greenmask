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

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var SetNullTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"SetNull",
		"Set NULL value",
	),
	NewSetNullTransformer,
	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetNullable(true),
	).SetRequired(true),
)

type SetNullTransformer struct {
	columnName      string
	columnIdx       int
	affectedColumns map[int]string
}

func NewSetNullTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string

	p := parameters["column"]
	if _, err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	return &SetNullTransformer{
		columnName:      columnName,
		columnIdx:       idx,
		affectedColumns: affectedColumns,
	}, nil, nil
}

func (sut *SetNullTransformer) GetAffectedColumns() map[int]string {
	return sut.affectedColumns
}

func (sut *SetNullTransformer) Init(ctx context.Context) error {
	return nil
}

func (sut *SetNullTransformer) Done(ctx context.Context) error {
	return nil
}

func (sut *SetNullTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	if err := r.SetColumnValueByIdx(sut.columnIdx, toolkit.NewValue(nil, true)); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(SetNullTransformerDefinition)
}
