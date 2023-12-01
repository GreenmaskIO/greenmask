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

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var RandomChoiceTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"RandomChoice",
		"Replace values chosen randomly from list",
	),

	NewRandomChoiceTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"values",
		`list of value to randomly in string format. The string with value "\N" supposed to be NULL value`,
	).SetRequired(true),

	toolkit.MustNewParameter(
		"validate",
		`perform encode-decode procedure using column type, ensuring that value has correct type`,
	).SetRequired(false).
		SetDefaultValue(toolkit.ParamsValue("true")),

	toolkit.MustNewParameter(
		"keep_null",
		"do not replace NULL values to random value",
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
		return nil, nil, fmt.Errorf(`unable to parse "validate" param and values uniquness: %w`, err)
	}

	var keepNull bool
	p = parameters["keep_null"]
	if _, err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	p = parameters["values"]
	var values []string
	if _, err := p.Scan(&values); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "values" param: %w`, err)
	}
	rawValue := make([]*toolkit.RawValue, len(values))
	addedValues := make(map[string]struct{})
	for _, v := range values {
		if idx, ok := addedValues[v]; ok && validate {
			return nil,
				toolkit.ValidationWarnings{
					toolkit.NewValidationWarning().
						AddMeta("ParameterName", "values").
						AddMeta(fmt.Sprintf("ParameterItemValue[%d]", idx), v).
						SetMsg("value already exist in the list"),
				},
				nil
		}

		if validate {
			if v != defaultNullSeq {
				if err := validateValue([]byte(v), driver, idx); err != nil {
					return nil, nil, fmt.Errorf(`error validating value "%s": %w`, v, err)
				}
			}
		}

		if v == defaultNullSeq {
			rawValue = append(rawValue, toolkit.NewRawValue(nil, true))
		} else {
			rawValue = append(rawValue, toolkit.NewRawValue([]byte(v), false))
		}
	}

	return &RandomChoiceTransformer{
		columnName:      columnName,
		columnIdx:       idx,
		values:          rawValue,
		validate:        validate,
		affectedColumns: affectedColumns,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		keepNull:        keepNull,
		length:          len(rawValue),
	}, nil, nil
}

func (ht *RandomChoiceTransformer) GetAffectedColumns() map[int]string {
	return ht.affectedColumns
}

func (ht *RandomChoiceTransformer) Init(ctx context.Context) error {
	return nil
}

func (ht *RandomChoiceTransformer) Done(ctx context.Context) error {
	return nil
}

func (rct *RandomChoiceTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawAttributeValueByIdx(rct.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rct.keepNull {
		return r, nil
	}

	if err = r.SetRawAttributeValueByIdx(rct.columnIdx, rct.values[rct.rand.Intn(rct.length)]); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomChoiceTransformerDefinition)
}
