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

const defaultNullSeq = `\N`

var DictTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"Dict",
		"Replace values matched by dictionary keys",
	),

	NewDictTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"values",
		`map of value to replace in format: {"string": "string"}". The string with value "\N" supposed to be NULL value`,
	).SetRequired(true),

	toolkit.MustNewParameter(
		"default",
		`default value if not any value has been matched with dict. The string with value "\N" supposed to be NULL value. Default is empty`,
	).SetRequired(false),

	toolkit.MustNewParameter(
		"fail_not_matched",
		`fail if value is not matched with dict otherwise keep value`,
	).SetRequired(false).
		SetDefaultValue(toolkit.ParamsValue("false")),
	toolkit.MustNewParameter(
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
	ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter,
) (utils.Transformer, toolkit.ValidationWarnings, error) {
	p := parameters["column"]
	var columnName string
	if _, err := p.Scan(&columnName); err != nil {
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
	if _, err := p.Scan(&validate); err != nil {
		return nil, nil, fmt.Errorf(`unable to parse "validate" param: %w`, err)
	}

	p = parameters["values"]
	values := make(map[string]string)
	if _, err := p.Scan(&values); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "values" param: %w`, err)
	}
	dict := make(map[string]*toolkit.RawValue, len(values))
	for key, value := range values {
		if validate {
			// Validate key
			if key != defaultNullSeq {
				if err := validateValue([]byte(key), driver, idx); err != nil {
					return nil, nil, fmt.Errorf(`error validating key "%s": %w`, key, err)
				}
			}

			// Validate value
			if value != defaultNullSeq {
				if err := validateValue([]byte(value), driver, idx); err != nil {
					return nil, nil, fmt.Errorf(`error validating value "%s": %w`, value, err)
				}
			}

		}

		if value == defaultNullSeq {
			dict[key] = toolkit.NewRawValue(nil, true)
		} else {
			dict[key] = toolkit.NewRawValue([]byte(value), false)
		}
	}

	var defaultValue *toolkit.RawValue
	p = parameters["default"]
	var defaultValueStr string
	empty, err := p.Scan(&defaultValueStr)
	if err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "default" param: %w`, err)
	}
	if !empty {
		if validate {
			// Validate defaultValueStr
			if defaultValueStr != defaultNullSeq {
				if err := validateValue([]byte(defaultValueStr), driver, idx); err != nil {
					return nil, nil, fmt.Errorf(`error validating "default_value" "%s": %w`, defaultValueStr, err)
				}
			}
		}
		if defaultValueStr == defaultNullSeq {
			defaultValue = toolkit.NewRawValue(nil, true)
		} else {
			defaultValue = toolkit.NewRawValue([]byte(defaultValueStr), false)
		}
	}

	p = parameters["fail_not_matched"]
	var failNotMatched bool
	if _, err := p.Scan(&failNotMatched); err != nil {
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
	}, nil, nil
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
	val, err = r.GetRawAttributeValueByIdx(ht.columnIdx)
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

	if err = r.SetRawAttributeValueByIdx(ht.columnIdx, newVal); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func validateValue(data []byte, driver *toolkit.Driver, columnIdx int) error {
	_, err := driver.DecodeAttrByIdx(columnIdx, data)
	if err != nil {
		return fmt.Errorf(`"unable to decode value: %w"`, err)
	}
	return nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(DictTransformerDefinition)
}
