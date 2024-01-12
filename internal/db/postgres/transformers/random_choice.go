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
	"math/rand"
	"time"

	"github.com/tidwall/gjson"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var RandomChoiceTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"RandomChoice",
		"Replace values chosen randomly from list",
	),

	NewRandomChoiceTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"values",
		`list of values in any format. The string with value "\N" supposed to be NULL value`,
	).SetRequired(true).
		SetUnmarshaller(randomChoiceValuesUnmarshaller),

	toolkit.MustNewParameterDefinition(
		"validate",
		`perform decode procedure via PostgreSQL driver using column type, ensuring that value has correct type`,
	).SetRequired(false).
		SetDefaultValue(toolkit.ParamsValue("true")),

	toolkit.MustNewParameterDefinition(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(toolkit.ParamsValue("true")),
)

type RandomChoiceTransformer struct {
	columnName      string
	columnIdx       int
	values          []*toolkit.RawValue
	validate        bool
	affectedColumns map[int]string
	rand            *rand.Rand
	keepNull        bool
	length          int
}

func NewRandomChoiceTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.ParameterDefinition,
) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var warnings toolkit.ValidationWarnings
	p := parameters["column"]
	var columnName string
	if _, err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to parse "column" param: %w`, err)
	}

	columnIdx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[columnIdx] = columnName

	p = parameters["validate"]
	var validate bool
	if _, err := p.Scan(&validate); err != nil {
		return nil, nil, fmt.Errorf(`unable to parse "validate" param and values uniquness: %w`, err)
	}

	var keepNull bool
	p = parameters["keep_null"]
	if _, err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	p = parameters["values"]
	var values []toolkit.ParamsValue
	if _, err := p.Scan(&values); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "values" param: %w`, err)
	}
	rawValue := make([]*toolkit.RawValue, 0, len(values))
	addedValues := make(map[string]struct{})
	for idx, v := range values {

		if _, ok := addedValues[string(v)]; ok && validate {
			warnings = append(warnings,
				toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
					AddMeta("ParameterName", "values").
					AddMeta(fmt.Sprintf("ParameterItemValue[%d]", idx), v).
					SetMsg("value already exist in the list"),
			)
		}

		if validate {
			if string(v) != defaultNullSeq {
				if err := validateValue(v, driver, columnIdx); err != nil {
					warnings = append(warnings,
						toolkit.NewValidationWarning().
							SetSeverity(toolkit.ErrorValidationSeverity).
							AddMeta("ParameterName", "values").
							AddMeta(fmt.Sprintf("ParameterItemValue[%d]", idx), string(v)).
							AddMeta("Error", err.Error()).
							SetMsg("error validating value: driver decoding error"),
					)
					continue
				}
			}
		}

		if string(v) == defaultNullSeq {
			rawValue = append(rawValue, toolkit.NewRawValue(nil, true))
		} else {
			rawValue = append(rawValue, toolkit.NewRawValue([]byte(v), false))
		}
	}

	return &RandomChoiceTransformer{
		columnName:      columnName,
		columnIdx:       columnIdx,
		values:          rawValue,
		validate:        validate,
		affectedColumns: affectedColumns,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		keepNull:        keepNull,
		length:          len(rawValue),
	}, warnings, nil
}

func (rct *RandomChoiceTransformer) GetAffectedColumns() map[int]string {
	return rct.affectedColumns
}

func (rct *RandomChoiceTransformer) Init(ctx context.Context) error {
	return nil
}

func (rct *RandomChoiceTransformer) Done(ctx context.Context) error {
	return nil
}

func (rct *RandomChoiceTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(rct.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rct.keepNull {
		return r, nil
	}

	if err = r.SetRawColumnValueByIdx(rct.columnIdx, rct.values[rct.rand.Intn(rct.length)]); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func getRawValue(data toolkit.ParamsValue) toolkit.ParamsValue {
	resResponse := gjson.GetBytes(data, "@this")
	switch v := resResponse.Value().(type) {
	case string:
		return toolkit.ParamsValue(v)
	default:
		return data
	}
}

func randomChoiceValuesUnmarshaller(parameter *toolkit.ParameterDefinition, driver *toolkit.Driver, src toolkit.ParamsValue) (any, error) {
	var res []toolkit.ParamsValue
	getResult := gjson.GetBytes(src, "@this")
	if !getResult.Exists() {
		return nil, fmt.Errorf("error parsing raw value: value is empty or has wrong format")
	}
	if !getResult.IsArray() {
		return nil, fmt.Errorf("error parsing raw value: value is not an array")
	}

	for _, i := range getResult.Array() {
		switch v := i.Value().(type) {
		case string:
			res = append(res, toolkit.ParamsValue(v))
		default:
			res = append(res, toolkit.ParamsValue(i.Raw))
		}
	}
	return &res, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomChoiceTransformerDefinition)
}
