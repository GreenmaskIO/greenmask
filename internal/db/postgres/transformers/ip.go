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
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var IpTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties("IpTransformer", "Generate ip in the provided subnet"),

	NewIpTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "inet"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"subnet",
		"cidr subnet",
	).SetRequired(true),

	keepNullParameterDefinition,

	engineParameterDefinition,
)

type IpTransformer struct {
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
	columnIdx       int
	t               *transformers.IpAddress
}

func NewIpTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName, engine, subnet string
	var keepNull bool
	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
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

	p = parameters["subnet"]
	if err := p.Scan(&subnet); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "subnet" param: %w`, err)
	}

	t := transformers.NewIpAddress(subnet)
	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &IpTransformer{
		columnName:      columnName,
		keepNull:        keepNull,
		affectedColumns: affectedColumns,
		columnIdx:       idx,
		t:               t,
	}, nil, nil
}

func (rbt *IpTransformer) GetAffectedColumns() map[int]string {
	return rbt.affectedColumns
}

func (rbt *IpTransformer) Init(ctx context.Context) error {
	return nil
}

func (rbt *IpTransformer) Done(ctx context.Context) error {
	return nil
}

func (rbt *IpTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(rbt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rbt.keepNull {
		return r, nil
	}

	ipVal, err := rbt.t.Generate()
	if err != nil {
		return nil, fmt.Errorf("unable to transform value: %w", err)
	}

	if err = r.SetColumnValueByIdx(rbt.columnIdx, ipVal); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(IpTransformerDefinition)
}
