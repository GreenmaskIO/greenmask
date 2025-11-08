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

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/generators/transformers"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

const RandomIpTransformerName = "RandomIp"

var RandomIPDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		RandomIpTransformerName,
		"Generate V4 or V6 IP in the provided subnet",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewIpTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"Column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypeClasses(
			commonmodels.TypeClassText,
			commonmodels.TypeClassInet,
		),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"subnet",
		"Subnet for generating random ip in V4 or V6 format",
	).SetRequired(true).
		SetSupportTemplate(true).
		SetDynamicMode(
			commonparameters.NewDynamicModeProperties().
				SetColumnProperties(
					commonparameters.NewColumnProperties().
						SetAllowedColumnTypeClasses(
							commonmodels.TypeClassText,
							commonmodels.TypeClassCidr,
						),
				),
		).SetUnmarshaler(
		func(_ *commonparameters.ParameterDefinition, _ commonininterfaces.DBMSDriver, src commonmodels.ParamsValue) (any, error) {
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
	subnetParam     commonparameters.Parameterizer
}

func NewIpTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {

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

func (rbt *RandomIp) GetAffectedColumns() map[int]string {
	return rbt.affectedColumns
}

func (rbt *RandomIp) Init(context.Context) error {
	return nil
}

func (rbt *RandomIp) Done(context.Context) error {
	return nil
}

func (rbt *RandomIp) Transform(_ context.Context, r commonininterfaces.Recorder) error {

	val, err := r.GetRawColumnValueByIdx(rbt.columnIdx)
	if err != nil {
		return fmt.Errorf("unable to scan value: %w", err)
	}

	var subnet *net.IPNet
	if rbt.dynamicMode {
		subnet = &net.IPNet{}
		if err = rbt.subnetParam.Scan(subnet); err != nil {
			return fmt.Errorf(`unable to scan "subnet" param: %w`, err)
		}
	}

	ipVal, err := rbt.t.Generate(val.Data, subnet)
	if err != nil {
		return fmt.Errorf("unable to transform value: %w", err)
	}

	newVal := commonmodels.NewColumnRawValue([]byte(ipVal.String()), false)
	if err = r.SetRawColumnValueByIdx(rbt.columnIdx, newVal); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}
	return nil
}
