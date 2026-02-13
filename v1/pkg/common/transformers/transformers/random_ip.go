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

	"github.com/greenmaskio/greenmask/v1/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/pkg/common/models"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/generators/transformers"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/utils"
)

const TransformerNameRandomIp = "RandomIp"

var RandomIPDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TransformerNameRandomIp,
		"Generate V4 or V6 IP in the provided subnet",
	).AddMeta(utils.AllowApplyForReferenced, true).
		AddMeta(utils.RequireHashEngineParameter, true),

	NewIpTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"Column name",
	).SetIsColumn(parameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypeClasses(
			models.TypeClassText,
			models.TypeClassInet,
		),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"subnet",
		"Subnet for generating random ip in V4 or V6 format",
	).SetRequired(true).
		SetSupportTemplate(true).
		SetDynamicMode(
			parameters.NewDynamicModeProperties().
				SetColumnProperties(
					parameters.NewColumnProperties().
						SetAllowedColumnTypeClasses(
							models.TypeClassText,
							models.TypeClassCidr,
						),
				),
		).SetUnmarshaler(
		func(_ *parameters.ParameterDefinition, _ interfaces.DBMSDriver, src models.ParamsValue) (any, error) {
			dest := &net.IPNet{}
			err := scanIPNet(src, dest)
			return dest, err
		}),

	defaultEngineParameterDefinition,
)

type RandomIp struct {
	columnName      string
	affectedColumns map[int]string
	columnIdx       int
	dynamicMode     bool
	t               *transformers.IpAddress
	subnetParam     parameters.Parameterizer
}

func NewIpTransformer(
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
) (interfaces.Transformer, error) {

	subnetParam := parameters["subnet"]
	var subnet *net.IPNet

	dynamicMode := isInDynamicMode(parameters)
	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	engine, err := getParameterValueWithName[string](ctx, parameters, ParameterNameEngine)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	if subnetParam.IsDynamic() {
		dynamicMode = true
	} else {
		subnet = &net.IPNet{}
		if err := subnetParam.Scan(subnet); err != nil {
			return nil, fmt.Errorf(`unable to scan "subnet" param: %w`, err)
		}
	}

	t, err := transformers.NewIpAddress(subnet)
	if err != nil {
		return nil, fmt.Errorf("unable to create ip transformer: %w", err)
	}
	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &RandomIp{
		columnName: columnName,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx:   column.Idx,
		t:           t,
		subnetParam: subnetParam,
		dynamicMode: dynamicMode,
	}, nil
}

func (t *RandomIp) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *RandomIp) Init(context.Context) error {
	return nil
}

func (t *RandomIp) Done(context.Context) error {
	return nil
}

func (t *RandomIp) Transform(_ context.Context, r interfaces.Recorder) error {

	val, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("unable to scan value: %w", err)
	}

	var subnet *net.IPNet
	if t.dynamicMode {
		subnet = &net.IPNet{}
		if err = t.subnetParam.Scan(subnet); err != nil {
			return fmt.Errorf(`unable to scan "subnet" param: %w`, err)
		}
	}

	ipVal, err := t.t.Generate(val.Data, subnet)
	if err != nil {
		return fmt.Errorf("unable to transform value: %w", err)
	}

	newVal := models.NewColumnRawValue([]byte(ipVal.String()), false)
	if err = r.SetRawColumnValueByIdx(t.columnIdx, newVal); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}
	return nil
}

func (t *RandomIp) Describe() string {
	return TransformerNameRandomIp
}
