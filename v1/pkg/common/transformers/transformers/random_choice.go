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
	"errors"
	"fmt"

	"github.com/greenmaskio/greenmask/v1/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/pkg/common/models"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/generators/transformers"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/pkg/common/validationcollector"
	"github.com/tidwall/gjson"
)

const TransformerNameRandomChoice = "RandomChoice"

var ChoiceTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TransformerNameRandomChoice,
		"Replace values chosen randomly from list",
	).AddMeta(utils.AllowApplyForReferenced, true).
		AddMeta(utils.RequireHashEngineParameter, true),

	NewRandomChoiceTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(parameters.NewColumnProperties().
		SetAffected(true),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"values",
		`list of values in any format. The string with value "\N" supposed to be NULL value`,
	).SetRequired(true).
		SetUnmarshaler(randomChoiceValuesUnmarshaller),

	parameters.MustNewParameterDefinition(
		"validate",
		`perform decode procedure DBMS driver using column type, ensuring that value has correct type`,
	).SetRequired(false).
		SetDefaultValue(models.ParamsValue("true")),

	defaultKeepNullParameterDefinition,

	defaultEngineParameterDefinition,
)

type ChoiceTransformer struct {
	t               *transformers.RandomChoiceTransformer
	columnName      string
	columnIdx       int
	validate        bool
	affectedColumns map[int]string
	keepNull        bool
}

func NewRandomChoiceTransformer(
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

	keepNull, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameKeepNull)
	if err != nil {
		return nil, fmt.Errorf("get \"keep_null\" param: %w", err)
	}

	validate, err := getParameterValueWithName[bool](ctx, parameters, "validate")
	if err != nil {
		return nil, fmt.Errorf("get \"validate\" param: %w", err)
	}

	values, err := getParameterValueWithName[[]models.ParamsValue](ctx, parameters, "values")
	if err != nil {
		return nil, fmt.Errorf("get \"values\" param: %w", err)
	}

	if validate {
		if err := validateValues(ctx, values, tableDriver, columnName); err != nil {
			return nil, fmt.Errorf("validate values: %w", err)
		}
	}

	rawValues := make([]*models.ColumnRawValue, 0, len(values))
	for _, v := range values {
		if string(v) == defaultNullSeq {
			rawValues = append(rawValues, models.NewColumnRawValue(nil, true))
		} else {
			rawValues = append(rawValues, models.NewColumnRawValue(v, false))
		}
	}

	t := transformers.NewRandomChoiceTransformer(rawValues)

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &ChoiceTransformer{
		t:          t,
		columnName: columnName,
		columnIdx:  column.Idx,
		validate:   validate,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		keepNull: keepNull,
	}, nil
}

func (t *ChoiceTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *ChoiceTransformer) Init(context.Context) error {
	return nil
}

func (t *ChoiceTransformer) Done(context.Context) error {
	return nil
}

func (t *ChoiceTransformer) Transform(_ context.Context, r interfaces.Recorder) error {
	val, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if val.IsNull && t.keepNull {
		return nil
	}

	val, err = t.t.Transform(val.Data)
	if err != nil {
		return fmt.Errorf("transform value: %w", err)
	}

	if err = r.SetRawColumnValueByIdx(t.columnIdx, val); err != nil {
		return fmt.Errorf("set new value: %w", err)
	}

	return nil
}

func (t *ChoiceTransformer) Describe() string {
	return TransformerNameRandomChoice
}

var (
	errRandomChoiceValuesUnmarshallerWrongFormat = errors.New("value is empty or has wrong format")
	errRandomChoiceValuesUnmarshallerNotArray    = errors.New("value is not an array")
)

func randomChoiceValuesUnmarshaller(
	_ *parameters.ParameterDefinition,
	_ interfaces.DBMSDriver,
	src models.ParamsValue,
) (any, error) {
	var res []models.ParamsValue
	getResult := gjson.GetBytes(src, "@this")
	if !getResult.Exists() {
		return nil, errRandomChoiceValuesUnmarshallerWrongFormat
	}
	if !getResult.IsArray() {
		return nil, errRandomChoiceValuesUnmarshallerNotArray
	}

	for _, i := range getResult.Array() {
		switch v := i.Value().(type) {
		case string:
			res = append(res, models.ParamsValue(v))
		default:
			res = append(res, models.ParamsValue(i.Raw))
		}
	}
	return &res, nil
}

func choiceValidateValueViaDriver(data []byte, tableDriver interfaces.TableDriver, columnName string) error {
	_, err := tableDriver.DecodeValueByColumnName(columnName, data)
	if err != nil {
		return fmt.Errorf(`"decode value: %w"`, err)
	}
	return nil
}

func validateValues(
	ctx context.Context,
	values []models.ParamsValue,
	tableDriver interfaces.TableDriver,
	columnName string,
) (err error) {
	addedValues := make(map[string]struct{})
	for idx, v := range values {
		if _, ok := addedValues[string(v)]; ok {
			validationcollector.FromContext(ctx).Add(
				models.NewValidationWarning().
					SetSeverity(models.ValidationSeverityError).
					AddMeta("ParameterName", "values").
					AddMeta(fmt.Sprintf("ParameterItemValue[%d]", idx), v).
					SetMsg("value already exist in the list"))
			err = models.ErrFatalValidationError
		}

		if string(v) == defaultNullSeq {
			continue
		}

		if validationErr := choiceValidateValueViaDriver(v, tableDriver, columnName); validationErr != nil {
			validationcollector.FromContext(ctx).Add(
				models.NewValidationWarning().
					SetSeverity(models.ValidationSeverityError).
					AddMeta("ParameterName", "values").
					AddMeta(fmt.Sprintf("ParameterItemValue[%d]", idx), string(v)).
					AddMeta("Error", validationErr.Error()).
					SetMsg("error validating value: driver decoding error"))
			err = models.ErrFatalValidationError
		}
	}
	return err
}
