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

const (
	castTypeNameIndividual = "individual"
	castTypeNameGroup      = "group"
	castTypeNameAny        = "any"
)

const (
	managementTypeNameUniversal = "universal"
	managementTypeNameLocal     = "local"
	managementTypeNameAny       = "any"
)

const RandomMacTransformerName = "RandomMac"

var RandomMacAddressDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		RandomMacTransformerName,
		"Generate random mac address",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewMacAddressTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"Column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypeClasses(
			commonmodels.TypeClassText,
			commonmodels.TypeClassMacAddress,
		),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"keep_original_vendor",
		"Keep original vendor. Default false",
	).SetRequired(false).
		SetDefaultValue(commonmodels.ParamsValue("false")),

	commonparameters.MustNewParameterDefinition(
		"cast_type",
		"Cast type, supported types are: individual, group, any.",
	).SetRequired(false).
		SetAllowedValues(
			commonmodels.ParamsValue(castTypeNameIndividual),
			commonmodels.ParamsValue(castTypeNameGroup),
			commonmodels.ParamsValue(castTypeNameAny),
		).
		SetDefaultValue(commonmodels.ParamsValue(castTypeNameAny)).
		SetUnmarshaler(scanCastType),

	commonparameters.MustNewParameterDefinition(
		"management_type",
		"Management type, supported types are: universal, local, any.",
	).SetRequired(false).SetAllowedValues(
		commonmodels.ParamsValue(managementTypeNameUniversal),
		commonmodels.ParamsValue(managementTypeNameLocal),
		commonmodels.ParamsValue(managementTypeNameAny),
	).SetDefaultValue(commonmodels.ParamsValue(managementTypeNameAny)).
		SetUnmarshaler(scanManagementType),

	defaultKeepNullParameterDefinition,

	defaultEngineParameterDefinition,
)

type RandomMac struct {
	columnName         string
	affectedColumns    map[int]string
	columnIdx          int
	keepOriginalVendor bool
	keepNull           bool
	castType           int
	managementType     int
	t                  *transformers.MacAddress
	originalMac        net.HardwareAddr
}

func NewMacAddressTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	engine, err := getParameterValueWithName[string](ctx, parameters, ParameterNameEngine)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	keepOriginalVendor, err := getParameterValueWithName[bool](ctx, parameters, "keep_original_vendor")
	if err != nil {
		return nil, fmt.Errorf("get \"keep_original_vendor\" param: %w", err)
	}

	castType, err := getParameterValueWithName[int](ctx, parameters, "cast_type")
	if err != nil {
		return nil, fmt.Errorf("get \"keep_original_vendor\" param: %w", err)
	}

	managementType, err := getParameterValueWithName[int](ctx, parameters, "management_type")
	if err != nil {
		return nil, fmt.Errorf("get \"management_type\" param: %w", err)
	}

	keepNull, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameKeepNull)
	if err != nil {
		return nil, fmt.Errorf("get \"keep_null\" param: %w", err)
	}

	t, err := transformers.NewMacAddress()
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

	return &RandomMac{
		columnName: columnName,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		keepNull:           keepNull,
		columnIdx:          column.Idx,
		t:                  t,
		keepOriginalVendor: keepOriginalVendor,
		castType:           castType,
		managementType:     managementType,
	}, nil
}

func (rbt *RandomMac) GetAffectedColumns() map[int]string {
	return rbt.affectedColumns
}

func (rbt *RandomMac) Init(context.Context) error {
	return nil
}

func (rbt *RandomMac) Done(context.Context) error {
	return nil
}

func (rbt *RandomMac) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	rawVal, err := r.GetRawColumnValueByIdx(rbt.columnIdx)
	if err != nil {
		return fmt.Errorf("unable to scan value: %w", err)
	}

	if rawVal.IsNull && rbt.keepNull {
		return nil
	}

	if err := scanMacAddr(rawVal.Data, &rbt.originalMac); err != nil {
		return fmt.Errorf("unable to scan mac address: %w", err)
	}

	macAddr, err := rbt.t.Generate(rbt.originalMac, rbt.keepOriginalVendor, rbt.castType, rbt.managementType)
	if err != nil {
		return fmt.Errorf("unable to transform value: %w", err)
	}

	newVal := commonmodels.NewColumnRawValue([]byte(macAddr.String()), false)
	if err = r.SetRawColumnValueByIdx(rbt.columnIdx, newVal); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}

	return nil
}

func scanCastType(_ *commonparameters.ParameterDefinition, _ commonininterfaces.DBMSDriver, src commonmodels.ParamsValue) (any, error) {
	var res int
	switch string(src) {
	case castTypeNameIndividual:
		res = transformers.CastTypeIndividual
	case castTypeNameGroup:
		res = transformers.CastTypeGroup
	case castTypeNameAny:
		res = transformers.CastTypeAny
	default:
		return fmt.Errorf("unknow value %s", string(src)), nil
	}
	return &res, nil
}

func scanManagementType(_ *commonparameters.ParameterDefinition, _ commonininterfaces.DBMSDriver, src commonmodels.ParamsValue) (any, error) {
	var res int
	switch string(src) {
	case managementTypeNameUniversal:
		res = transformers.ManagementTypeUniversal
	case managementTypeNameLocal:
		res = transformers.ManagementTypeLocal
	case managementTypeNameAny:
		res = transformers.ManagementTypeAny
	default:
		return fmt.Errorf("unknow value %s", string(src)), nil
	}
	return &res, nil
}
