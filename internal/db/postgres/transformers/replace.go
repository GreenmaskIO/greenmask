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

var ReplaceTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"Replace",
		"Replace column value to the provided",
	),

	NewReplaceTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"value",
		"value to replace",
	).SetRequired(true).
		SetLinkParameter("column"),

	toolkit.MustNewParameter(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(toolkit.ParamsValue("true")),

	toolkit.MustNewParameter(
		"validate",
		"validate the value via driver decoding procedure",
	).SetDefaultValue(toolkit.ParamsValue("true")),
)

type ReplaceTransformer struct {
	columnName      string
	columnIdx       int
	keepNull        bool
	value           any
	rawValue        *toolkit.RawValue
	affectedColumns map[int]string
}

func NewReplaceTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName string
	var keepNull, validate bool

	p := parameters["column"]
	if _, err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, c, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf(`column with name "%s" is not found`, columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["validate"]
	if _, err := p.Scan(&validate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "validate" param: %w`, err)
	}

	value := parameters["value"].RawValue()
	if validate {
		_, err := driver.DecodeValueByTypeOid(uint32(c.TypeOid), value)
		if err != nil {
			return nil, toolkit.ValidationWarnings{
				toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
					SetMsg("error decoding \"value\" parameter from raw string to type").
					AddMeta("TypeName", c.TypeName).
					AddMeta("ParameterName", "value").
					AddMeta("ParameterValue", string(value)).
					AddMeta("Error", err.Error()),
			}, nil
		}
	}

	p = parameters["keep_null"]
	if _, err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &ReplaceTransformer{
		columnName:      columnName,
		keepNull:        keepNull,
		value:           value,
		affectedColumns: affectedColumns,
		rawValue:        toolkit.NewRawValue(value, false),
		columnIdx:       idx,
	}, nil, nil
}

func (rt *ReplaceTransformer) GetAffectedColumns() map[int]string {
	return rt.affectedColumns
}

func (rt *ReplaceTransformer) Init(ctx context.Context) error {
	return nil
}

func (rt *ReplaceTransformer) Done(ctx context.Context) error {
	return nil
}

func (rt *ReplaceTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(rt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rt.keepNull {
		return r, nil
	}

	if err := r.SetRawColumnValueByIdx(rt.columnIdx, rt.rawValue); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(ReplaceTransformerDefinition)
}
