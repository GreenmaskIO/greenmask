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

	"github.com/greenmaskio/greenmask/pkg/toolkit"

	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

const defaultNullSeq = `\N`

const DictTransformerName = "Dict"

var DictTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		DictTransformerName,
		"Replace values matched by dictionary keys",
	),

	NewDictTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(parameters.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"values",
		`map of value to replace in format: {"string": "string"}". The string with value "\N" supposed to be NULL value`,
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"default",
		`default value if not any value has been matched with dict. The string with value "\N" supposed to be NULL value. Default is empty`,
	).SetRequired(false),

	parameters.MustNewParameterDefinition(
		"fail_not_matched",
		`fail if value is not matched with dict otherwise keep value`,
	).SetRequired(false).
		SetDefaultValue(toolkit.ParamsValue("true")),

	parameters.MustNewParameterDefinition(
		"validate",
		`perform encode-decode procedure using column type, ensuring that value has correct type`,
	).SetRequired(false).
		SetDefaultValue(toolkit.ParamsValue("true")),
)

type DictTransformer struct {
	columnName      string
	columnIdx       int
	dict            map[string]*toolkit.RawValue
	defaultValue    *toolkit.RawValue
	failNotMatched  bool
	validate        bool
	affectedColumns map[int]string
}

func NewDictTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer,
) (utils.Transformer, toolkit.ValidationWarnings, error) {
	p := parameters["column"]
	var columnName string
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to parse "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["validate"]
	var validate bool
	if err := p.Scan(&validate); err != nil {
		return nil, nil, fmt.Errorf(`unable to parse "validate" param: %w`, err)
	}

	p = parameters["values"]
	values := make(map[string]string)
	if err := p.Scan(&values); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "values" param: %w`, err)
	}
	dict := make(map[string]*toolkit.RawValue, len(values))
	var warnings toolkit.ValidationWarnings
	for key, value := range values {
		if validate {
			// Validate key
			if key != defaultNullSeq {
				if err := dictValidateValue([]byte(key), driver, idx); err != nil {
					warnings = append(warnings,
						toolkit.NewValidationWarning().
							SetSeverity(toolkit.ErrorValidationSeverity).
							AddMeta("KeyValue", key).
							AddMeta("Error", err.Error()).
							AddMeta("ParameterName", "values").
							SetMsg("error validating values: error encoding key"),
					)
				}
			}

			// Validate value
			if string(value) != defaultNullSeq {
				if err := dictValidateValue([]byte(value), driver, idx); err != nil {
					warnings = append(warnings,
						toolkit.NewValidationWarning().
							SetSeverity(toolkit.ErrorValidationSeverity).
							AddMeta("ValueValue", value).
							AddMeta("ParameterName", "values").
							AddMeta("Error", err.Error()).
							SetMsg("error validating values: error encoding value"),
					)
				}
			}

		}

		if string(value) == defaultNullSeq {
			dict[key] = toolkit.NewRawValue(nil, true)
		} else {
			dict[key] = toolkit.NewRawValue([]byte(value), false)
		}
	}

	var defaultValue *toolkit.RawValue
	p = parameters["default"]

	isEmpty, err := p.IsEmpty()
	if err != nil {
		return nil, nil, fmt.Errorf("error checking is parameter \"default\" empty: %w", err)
	}

	if !isEmpty {
		rawDefaultValue, err := p.RawValue()
		if err != nil {
			return nil, nil, fmt.Errorf(`unable to scan "default" param: %w`, err)
		}

		if string(rawDefaultValue) == defaultNullSeq {
			defaultValue = toolkit.NewRawValue(nil, true)
		} else {
			defaultValue = toolkit.NewRawValue(rawDefaultValue, false)
		}

		if validate {
			// Validate defaultValueStr
			if !defaultValue.IsNull {
				if err := dictValidateValue(defaultValue.Data, driver, idx); err != nil {
					warnings = append(warnings,
						toolkit.NewValidationWarning().
							SetSeverity(toolkit.ErrorValidationSeverity).
							AddMeta("ParameterValue", string(defaultValue.Data)).
							AddMeta("ParameterName", "default").
							AddMeta("Error", err.Error()).
							SetMsg("error validating \"default\""),
					)
				}
			}
		}

	}

	p = parameters["fail_not_matched"]
	var failNotMatched bool
	if err := p.Scan(&failNotMatched); err != nil {
		return nil, nil, fmt.Errorf(`unable to parse "fail_not_matched" param: %w`, err)
	}

	return &DictTransformer{
		dict:            dict,
		defaultValue:    defaultValue,
		validate:        validate,
		failNotMatched:  failNotMatched,
		columnName:      columnName,
		columnIdx:       idx,
		affectedColumns: affectedColumns,
	}, warnings, nil
}

func (ht *DictTransformer) GetAffectedColumns() map[int]string {
	return ht.affectedColumns
}

func (ht *DictTransformer) Init(ctx context.Context) error {
	return nil
}

func (ht *DictTransformer) Done(ctx context.Context) error {
	return nil
}

func (ht *DictTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var val *toolkit.RawValue
	var err error
	val, err = r.GetRawColumnValueByIdx(ht.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}

	var newVal *toolkit.RawValue
	var isSet bool
	if val.IsNull {
		newVal, isSet = ht.dict[defaultNullSeq]
	} else {
		newVal, isSet = ht.dict[string(val.Data)]
	}

	if !isSet {
		if ht.defaultValue != nil {
			newVal = ht.defaultValue
		} else if ht.failNotMatched {
			return nil, fmt.Errorf(`unable to match value for "%s"`, string(val.Data))
		}
	}

	if err = r.SetRawColumnValueByIdx(ht.columnIdx, newVal); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func dictValidateValue(data []byte, driver *toolkit.Driver, columnIdx int) error {
	_, err := driver.DecodeValueByColumnIdx(columnIdx, data)
	if err != nil {
		return fmt.Errorf(`"unable to decode value: %w"`, err)
	}
	return nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(DictTransformerDefinition)
}
