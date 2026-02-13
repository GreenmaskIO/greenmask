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

const TransformerNameRandomMac = "RandomMac"

var RandomMacAddressDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TransformerNameRandomMac,
		"Generate random mac address",
	).AddMeta(utils.AllowApplyForReferenced, true).
		AddMeta(utils.RequireHashEngineParameter, true),

	NewMacAddressTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"Column name",
	).SetIsColumn(parameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypeClasses(
			models.TypeClassText,
			models.TypeClassMacAddress,
		),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"keep_original_vendor",
		"Keep original vendor. Default false",
	).SetRequired(false).
		SetDefaultValue(models.ParamsValue("false")),

	parameters.MustNewParameterDefinition(
		"cast_type",
		"Cast type, supported types are: individual, group, any.",
	).SetRequired(false).
		SetAllowedValues(
			models.ParamsValue(castTypeNameIndividual),
			models.ParamsValue(castTypeNameGroup),
			models.ParamsValue(castTypeNameAny),
		).
		SetDefaultValue(models.ParamsValue(castTypeNameAny)).
		SetUnmarshaler(scanCastType),

	parameters.MustNewParameterDefinition(
		"management_type",
		"Management type, supported types are: universal, local, any.",
	).SetRequired(false).SetAllowedValues(
		models.ParamsValue(managementTypeNameUniversal),
		models.ParamsValue(managementTypeNameLocal),
		models.ParamsValue(managementTypeNameAny),
	).SetDefaultValue(models.ParamsValue(managementTypeNameAny)).
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
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
) (interfaces.Transformer, error) {
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

func (t *RandomMac) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *RandomMac) Init(context.Context) error {
	return nil
}

func (t *RandomMac) Done(context.Context) error {
	return nil
}

func (t *RandomMac) Transform(_ context.Context, r interfaces.Recorder) error {
	rawVal, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("unable to scan value: %w", err)
	}

	if rawVal.IsNull && t.keepNull {
		return nil
	}

	if err := scanMacAddr(rawVal.Data, &t.originalMac); err != nil {
		return fmt.Errorf("unable to scan mac address: %w", err)
	}

	macAddr, err := t.t.Generate(t.originalMac, t.keepOriginalVendor, t.castType, t.managementType)
	if err != nil {
		return fmt.Errorf("unable to transform value: %w", err)
	}

	newVal := models.NewColumnRawValue([]byte(macAddr.String()), false)
	if err = r.SetRawColumnValueByIdx(t.columnIdx, newVal); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}

	return nil
}

func (t *RandomMac) Describe() string {
	return TransformerNameRandomMac
}

func scanCastType(_ *parameters.ParameterDefinition, _ interfaces.DBMSDriver, src models.ParamsValue) (any, error) {
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

func scanManagementType(_ *parameters.ParameterDefinition, _ interfaces.DBMSDriver, src models.ParamsValue) (any, error) {
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
