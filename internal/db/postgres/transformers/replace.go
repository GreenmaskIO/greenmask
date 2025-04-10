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

const ReplaceTransformerName = "Replace"

var ReplaceTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		ReplaceTransformerName,
		"Replace column value to the provided",
	).AddMeta(AllowApplyForReferenced, true).
		AddMeta(RequireHashEngineParameter, true),

	NewReplaceTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetNullable(true),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"value",
		"value to replace",
	).SetRequired(true).
		SetSupportTemplate(true).
		SetLinkParameter("column").
		SetDynamicMode(
			toolkit.NewDynamicModeProperties(),
		),

	toolkit.MustNewParameterDefinition(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(toolkit.ParamsValue("true")),

	toolkit.MustNewParameterDefinition(
		"validate",
		"validate the value via driver decoding procedure",
	).SetDefaultValue(toolkit.ParamsValue("true")),
)

type transformFn func(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error)

type ReplaceTransformer struct {
	columnName          string
	columnIdx           int
	keepNull            bool
	rawValue            *toolkit.RawValue
	affectedColumns     map[int]string
	validate            bool
	columnOIDToValidate uint32

	columnParam   toolkit.Parameterizer
	valueParam    toolkit.Parameterizer
	validateParam toolkit.Parameterizer
	keepNullParam toolkit.Parameterizer

	// transform function to be used for transforming the record.
	//
	// Depending on the value parameter type, it can be either static or dynamic.
	transform transformFn
}

func NewReplaceTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName string
	var keepNull, validate bool

	columnParam := parameters["column"]
	if err := columnParam.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, c, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf(`column with name "%s" is not found`, columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	validateParam := parameters["validate"]
	if err := validateParam.Scan(&validate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "validate" param: %w`, err)
	}

	valueParam := parameters["value"]

	columnOIDToValidate := uint32(c.TypeOid)
	var rawValue *toolkit.RawValue
	if !valueParam.IsDynamic() {
		// Get value from parameter
		value, err := valueParam.RawValue()
		if err != nil {
			return nil, nil, fmt.Errorf(
				"error getting raw value from parameter \"%s\": %w",
				valueParam.GetDefinition().Name, err,
			)
		}
		// Validate the value if requested
		if validate {
			_, err = driver.DecodeValueByTypeOid(uint32(c.TypeOid), value)
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
		rawValue = toolkit.NewRawValue(value, false)
	}

	keepNullParam := parameters["keep_null"]
	if err := keepNullParam.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}
	t := &ReplaceTransformer{
		columnName:          columnName,
		keepNull:            keepNull,
		affectedColumns:     affectedColumns,
		rawValue:            rawValue,
		columnIdx:           idx,
		validate:            validate,
		columnOIDToValidate: columnOIDToValidate,

		columnParam:   columnParam,
		validateParam: validateParam,
		valueParam:    valueParam,
		keepNullParam: keepNullParam,
	}

	// Set the transform function based on the value parameter type.
	t.transform = t.transformStatic
	if valueParam.IsDynamic() {
		t.transform = t.transformDynamic
	}
	return t, nil, nil
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

func (rt *ReplaceTransformer) getValueToReplace(d *toolkit.Driver) (*toolkit.RawValue, error) {
	if !rt.valueParam.IsDynamic() {
		return rt.rawValue, nil
	}

	// Check if the current dynamic value is empty (is null).
	isEmpty, err := rt.valueParam.IsEmpty()
	if err != nil {
		return nil, fmt.Errorf("check if value is empty: %w", err)
	}
	if isEmpty {
		// if empty then create the toolkit.RawValue with null value
		// Note: we allow null values, though for dynamic values this may cause
		// 		 constraint violation due to unexpected null values that
		//       was got from the dynamic parameter.
		return toolkit.NewRawValue(nil, true), nil
	}
	// If not empty just get the raw value from the parameter
	v, err := rt.valueParam.RawValue()
	if err != nil {
		return nil, fmt.Errorf(
			"get raw value from parameter \"%s\": %w",
			rt.valueParam.GetDefinition().Name, err,
		)
	}
	// Validate the value if requested
	if rt.validate {
		_, err = d.DecodeValueByTypeOid(rt.columnOIDToValidate, v)
		if err != nil {
			return nil, fmt.Errorf("dynamic value validation error: %w", err)
		}
	}

	return toolkit.NewRawValue(v, false), nil
}

func (rt *ReplaceTransformer) transformDynamic(
	ctx context.Context,
	r *toolkit.Record,
) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(rt.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}

	if val.IsNull && rt.keepNull {
		return r, nil
	}

	newValue, err := rt.getValueToReplace(r.Driver)
	if err != nil {
		return nil, fmt.Errorf("get value to replace: %w", err)
	}

	if err := r.SetRawColumnValueByIdx(rt.columnIdx, newValue); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func (rt *ReplaceTransformer) transformStatic(
	ctx context.Context,
	r *toolkit.Record,
) (*toolkit.Record, error) {
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

func (rt *ReplaceTransformer) Transform(
	ctx context.Context,
	r *toolkit.Record,
) (*toolkit.Record, error) {
	return rt.transform(ctx, r)
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(ReplaceTransformerDefinition)
}
