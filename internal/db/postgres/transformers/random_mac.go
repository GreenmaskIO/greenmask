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

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
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

var RandomMacAddressDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		RandomMacTransformerName,
		"Generate random mac address",
	).AddMeta(AllowApplyForReferenced, true).
		AddMeta(RequireHashEngineParameter, true),

	NewMacAddressTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"Column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("macaddr", "varchar", "text"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"keep_original_vendor",
		"Keep original vendor. Default false",
	).SetRequired(false).
		SetDefaultValue(toolkit.ParamsValue("false")),

	toolkit.MustNewParameterDefinition(
		"cast_type",
		"Cast type, supported types are: individual, group, any.",
	).SetRequired(false).
		SetAllowedValues(
			toolkit.ParamsValue(castTypeNameIndividual),
			toolkit.ParamsValue(castTypeNameGroup),
			toolkit.ParamsValue(castTypeNameAny),
		).
		SetDefaultValue(toolkit.ParamsValue(castTypeNameAny)).
		SetUnmarshaler(scanCastType),

	toolkit.MustNewParameterDefinition(
		"management_type",
		"Management type, supported types are: universal, local, any.",
	).SetRequired(false).SetAllowedValues(
		toolkit.ParamsValue(managementTypeNameUniversal),
		toolkit.ParamsValue(managementTypeNameLocal),
		toolkit.ParamsValue(managementTypeNameAny),
	).SetDefaultValue(toolkit.ParamsValue(managementTypeNameAny)).
		SetUnmarshaler(scanManagementType),

	engineParameterDefinition,
)

type RandomMac struct {
	columnName         string
	affectedColumns    map[int]string
	columnIdx          int
	keepOriginalVendor bool
	castType           int
	managementType     int
	t                  *transformers.MacAddress
	originalMac        net.HardwareAddr
}

func NewMacAddressTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	keepOriginalVendorParam := parameters["keep_original_vendor"]
	castTypeParam := parameters["cast_type"]
	managementTypeParam := parameters["management_type"]
	engineParam := parameters["engine"]
	columnParam := parameters["column"]

	var columnName, engine string
	var castType, managementType int
	var keepOriginalVendor bool
	if err := columnParam.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	if err := engineParam.Scan(&engine); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "engine" param: %w`, err)
	}

	if err := keepOriginalVendorParam.Scan(&keepOriginalVendor); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_original_vendor" param: %w`, err)
	}

	if err := castTypeParam.Scan(&castType); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "cast_type" param: %w`, err)
	}

	if err := managementTypeParam.Scan(&managementType); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "management_type" param: %w`, err)
	}

	t, err := transformers.NewMacAddress()
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

	return &RandomMac{
		columnName:         columnName,
		affectedColumns:    affectedColumns,
		columnIdx:          idx,
		t:                  t,
		keepOriginalVendor: keepOriginalVendor,
		castType:           castType,
		managementType:     managementType,
	}, nil, nil
}

func (rbt *RandomMac) GetAffectedColumns() map[int]string {
	return rbt.affectedColumns
}

func (rbt *RandomMac) Init(ctx context.Context) error {
	return nil
}

func (rbt *RandomMac) Done(ctx context.Context) error {
	return nil
}

func (rbt *RandomMac) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	rawVal, err := r.GetRawColumnValueByIdx(rbt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	err = r.Driver.ScanValueByTypeOid(pgtype.MacaddrOID, rawVal.Data, &rbt.originalMac)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}

	macAddr, err := rbt.t.Generate(rbt.originalMac, rbt.keepOriginalVendor, rbt.castType, rbt.managementType)
	if err != nil {
		return nil, fmt.Errorf("unable to transform value: %w", err)
	}

	newVal := toolkit.NewRawValue([]byte(macAddr.String()), false)
	if err = r.SetRawColumnValueByIdx(rbt.columnIdx, newVal); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func scanCastType(parameter *toolkit.ParameterDefinition, driver *toolkit.Driver, src toolkit.ParamsValue) (any, error) {
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

func scanManagementType(parameter *toolkit.ParameterDefinition, driver *toolkit.Driver, src toolkit.ParamsValue) (any, error) {
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

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomMacAddressDefinition)
}
