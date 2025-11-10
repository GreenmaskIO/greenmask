// Copyright 2025 Greenmask
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

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

const (
	TransformerNameDict = "Dict"

	defaultNullSeq = `\N`
)

var ErrDictTransformerFailNotMatched = fmt.Errorf("value not matched with dict")

var DictTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		TransformerNameDict,
		"Replace values matched by dictionary keys",
	),
	NewDictTransformer,
	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),
	commonparameters.MustNewParameterDefinition(
		"values",
		`map of value to replace in format: {"string": "string"}". The string with value "\N" supposed to be NULL value`,
	).SetRequired(true),
	commonparameters.MustNewParameterDefinition(
		"default",
		`default value if not any value has been matched with dict. The string with value "\N" supposed to be NULL value. Default is empty`,
	).SetRequired(false),
	commonparameters.MustNewParameterDefinition(
		"fail_not_matched",
		`fail if value is not matched with dict otherwise keep value`,
	).SetRequired(false).
		SetDefaultValue(commonmodels.ParamsValue("true")),

	// validate parameter definition
	defaultValidateParameterDefinition,
)

type DictTransformer struct {
	columnName      string
	columnIdx       int
	dict            map[string]*commonmodels.ColumnRawValue
	defaultValue    *commonmodels.ColumnRawValue
	failNotMatched  bool
	validate        bool
	affectedColumns map[int]string
}

// dictValidateValue - validate value via table driver decode procedure.
func dictValidateValue(data []byte, tableDriver commonininterfaces.TableDriver, columnIdx int) error {
	_, err := tableDriver.DecodeValueByColumnIdx(columnIdx, data)
	if err != nil {
		return fmt.Errorf(`"unable to decode value: %w"`, err)
	}
	return nil
}

// validateKeyAndValue - validate key and value if they are not NULL sequence.
func validateKeyAndValue(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	columnIdx int,
	key string,
	value string,
) {
	if key != defaultNullSeq {
		if err := dictValidateValue([]byte(key), tableDriver, columnIdx); err != nil {
			validationcollector.FromContext(ctx).
				Add(commonmodels.NewValidationWarning().
					SetSeverity(commonmodels.ValidationSeverityError).
					AddMeta("KeyValue", key).
					AddMeta("Error", err.Error()).
					AddMeta("ParameterName", "values").
					SetMsg("error validating values: error encoding key"))
		}
	}

	// Validate value
	if string(value) != defaultNullSeq {
		if err := dictValidateValue([]byte(value), tableDriver, columnIdx); err != nil {
			validationcollector.FromContext(ctx).
				Add(commonmodels.NewValidationWarning().
					SetSeverity(commonmodels.ValidationSeverityError).
					AddMeta("ValueValue", value).
					AddMeta("ParameterName", "values").
					AddMeta("Error", err.Error()).
					SetMsg("error validating values: error encoding value"))
		}
	}
}

// geDefaultParameterValue - get the default parameter value if it is set.
// If the value is "\N" then it is considered as NULL value.
// If not set then nil is returned.
func geDefaultParameterValue(
	parameter commonparameters.Parameterizer,
) (res *commonmodels.ColumnRawValue, _ error) {
	isEmpty, err := parameter.IsEmpty()
	if err != nil {
		return nil, fmt.Errorf("error checking is parameter \"default\" empty: %w", err)
	}
	if isEmpty {
		return nil, nil
	}

	rawDefaultValue, err := parameter.RawValue()
	if err != nil {
		return nil, fmt.Errorf(`unable to scan "default" param: %w`, err)
	}
	if string(rawDefaultValue) == defaultNullSeq {
		res = commonmodels.NewColumnRawValue(nil, true)
	} else {
		res = commonmodels.NewColumnRawValue(rawDefaultValue, false)
	}
	return res, nil
}

func validateDefaultValue(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	columnIdx int,
	value *commonmodels.ColumnRawValue,
) {
	if value == nil {
		return
	}
	if value.IsNull {
		return
	}
	if err := dictValidateValue(value.Data, tableDriver, columnIdx); err != nil {
		validationcollector.FromContext(ctx).
			Add(commonmodels.NewValidationWarning().
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("ParameterValue", string(value.Data)).
				AddMeta("ParameterName", "default").
				AddMeta("Error", err.Error()).
				SetMsg("error validating \"default\""))
	}
}

func NewDictTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, err
	}
	validate, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameValidate)
	if err != nil {
		return nil, err
	}
	values, err := getParameterValueWithName[map[string]string](ctx, parameters, "values")
	if err != nil {
		return nil, err
	}

	dict := make(map[string]*commonmodels.ColumnRawValue, len(values))
	for key, value := range values {
		if validate {
			validateKeyAndValue(ctx, tableDriver, column.Idx, key, value)
		}

		dict[key] = commonmodels.NewColumnRawValue(nil, true)
		if string(value) != defaultNullSeq {
			dict[key] = commonmodels.NewColumnRawValue([]byte(value), false)
		}
	}

	defaultValue, err := geDefaultParameterValue(parameters["default"])
	if err != nil {
		return nil, err
	}
	if validate {
		validateDefaultValue(ctx, tableDriver, column.Idx, defaultValue)
	}

	failNotMatched, err := getParameterValueWithName[bool](ctx, parameters, "fail_not_matched")
	if err != nil {
		return nil, err
	}

	return &DictTransformer{
		dict:           dict,
		defaultValue:   defaultValue,
		validate:       validate,
		failNotMatched: failNotMatched,
		columnName:     columnName,
		columnIdx:      column.Idx,
		affectedColumns: map[int]string{
			column.Idx: column.Name,
		},
	}, nil
}

func (t *DictTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *DictTransformer) Init(context.Context) error {
	return nil
}

func (t *DictTransformer) Done(context.Context) error {
	return nil
}

func (t *DictTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	var val *commonmodels.ColumnRawValue
	var err error
	val, err = r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("unable to scan attribute value: %w", err)
	}

	var newVal *commonmodels.ColumnRawValue
	var isSet bool
	if val.IsNull {
		newVal, isSet = t.dict[defaultNullSeq]
	} else {
		newVal, isSet = t.dict[string(val.Data)]
	}

	if !isSet {
		switch {
		case t.defaultValue != nil:
			// If default value is set - use it.
			newVal = t.defaultValue
		case t.failNotMatched:
			// If fail if not matched - return error.
			// FIXME: we might not want to push it as an error. This might be a sensitive data.
			return fmt.Errorf(
				`unable to match value for "%s": %w`,
				val.String(), ErrDictTransformerFailNotMatched,
			)
		}
	}

	if err = r.SetRawColumnValueByIdx(t.columnIdx, newVal); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}

	return nil
}

func (t *DictTransformer) Describe() string {
	return TransformerNameDict
}
