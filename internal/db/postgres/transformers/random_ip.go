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
	"net"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const RandomIpTransformerName = "RandomIp"

var RandomIpDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		RandomIpTransformerName,
		"Generate V4 or V6 IP in the provided subnet",
	).AddMeta(AllowApplyForReferenced, true).
		AddMeta(RequireHashEngineParameter, true),

	NewIpTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"Column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar", "inet"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"subnet",
		"Subnet for generating random ip in V4 or V6 format",
	).SetRequired(true).
		SetSupportTemplate(true).
		SetCastDbType("cidr").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties().
				SetCompatibleTypes("text", "varchar", "cidr"),
		),

	engineParameterDefinition,
)

type RandomIp struct {
	columnName      string
	affectedColumns map[int]string
	columnIdx       int
	dynamicMode     bool
	t               *transformers.IpAddress
	subnetParam     toolkit.Parameterizer
}

func NewIpTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	subnetParam := parameters["subnet"]

	var columnName, engine string
	var subnet *net.IPNet
	var dynamicMode bool
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

	p = parameters["engine"]
	if err := p.Scan(&engine); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "engine" param: %w`, err)
	}

	if subnetParam.IsDynamic() {
		dynamicMode = true
	} else {
		subnet = &net.IPNet{}
		if err := subnetParam.Scan(subnet); err != nil {
			return nil, nil, fmt.Errorf(`unable to scan "subnet" param: %w`, err)
		}
	}

	t, err := transformers.NewIpAddress(subnet)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create ip transformer: %w", err)
	}
	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &RandomIp{
		columnName:      columnName,
		affectedColumns: affectedColumns,
		columnIdx:       idx,
		t:               t,
		subnetParam:     subnetParam,
		dynamicMode:     dynamicMode,
	}, nil, nil
}

func (rbt *RandomIp) GetAffectedColumns() map[int]string {
	return rbt.affectedColumns
}

func (rbt *RandomIp) Init(ctx context.Context) error {
	return nil
}

func (rbt *RandomIp) Done(ctx context.Context) error {
	return nil
}

func (rbt *RandomIp) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	val, err := r.GetRawColumnValueByIdx(rbt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}

	var subnet *net.IPNet
	if rbt.dynamicMode {
		subnet = &net.IPNet{}
		if err = rbt.subnetParam.Scan(subnet); err != nil {
			return nil, fmt.Errorf(`unable to scan "subnet" param: %w`, err)
		}
	}

	ipVal, err := rbt.t.Generate(val.Data, subnet)
	if err != nil {
		return nil, fmt.Errorf("unable to transform value: %w", err)
	}

	newVal := toolkit.NewRawValue([]byte(ipVal.String()), false)
	if err = r.SetRawColumnValueByIdx(rbt.columnIdx, newVal); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomIpDefinition)
}
