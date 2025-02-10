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
	"github.com/greenmaskio/greenmask/pkg/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const RandomUuidTransformerName = "RandomUuid"

var uuidTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		RandomUuidTransformerName,
		"Generate UUID",
	).AddMeta(AllowApplyForReferenced, true).
		AddMeta(RequireHashEngineParameter, true),

	NewRandomUuidTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar", "uuid"),
	).SetRequired(true),

	keepNullParameterDefinition,

	engineParameterDefinition,
)

type RandomUuidTransformer struct {
	t               *transformers.RandomUuidTransformer
	columnName      string
	columnIdx       int
	keepNull        bool
	affectedColumns map[int]string
}

func NewRandomUuidTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName, engine string
	var keepNull bool

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["keep_null"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	p = parameters["engine"]
	if err := p.Scan(&engine); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "engine" param: %w`, err)
	}

	t := transformers.NewRandomUuidTransformer()

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &RandomUuidTransformer{
		t:               t,
		columnName:      columnName,
		keepNull:        keepNull,
		affectedColumns: affectedColumns,
		columnIdx:       idx,
	}, nil, nil
}

func (rut *RandomUuidTransformer) GetAffectedColumns() map[int]string {
	return rut.affectedColumns
}

func (rut *RandomUuidTransformer) Init(ctx context.Context) error {
	return nil
}

func (rut *RandomUuidTransformer) Done(ctx context.Context) error {
	return nil
}

func (rut *RandomUuidTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(rut.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rut.keepNull {
		return r, nil
	}

	uuidVal, err := rut.t.Transform(val.Data)
	if err != nil {
		return nil, fmt.Errorf("error transforming value: %w", err)
	}

	data, err := uuidVal.MarshalText()
	if err != nil {
		return nil, fmt.Errorf("error umarshalling uuid: %w", err)
	}
	if err = r.SetRawColumnValueByIdx(rut.columnIdx, toolkit.NewRawValue(data, false)); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(uuidTransformerDefinition)
}
