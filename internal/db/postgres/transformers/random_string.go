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
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const RandomStringTransformerName = "RandomString"

var stringTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		RandomStringTransformerName,
		"Generate a string withing the specified length with provided char set",
	).AddMeta(AllowApplyForReferenced, true).
		AddMeta(RequireHashEngineParameter, true),

	NewRandomStringTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar", "char", "bpchar", "citext"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"min_length",
		"min length of string",
	).SetSupportTemplate(true).
		SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"max_length",
		"max length of string",
	).SetSupportTemplate(true).
		SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"symbols",
		"the characters range for random string",
	).SetDefaultValue(toolkit.ParamsValue("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")),

	keepNullParameterDefinition,

	engineParameterDefinition,
)

type RandomStringTransformer struct {
	t               *transformers.RandomStringTransformer
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
	columnIdx       int
}

func NewRandomStringTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {

	var columnName, symbols, engine string
	var minLength, maxLength int
	var keepNull bool

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["min_length"]
	if err := p.Scan(&minLength); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "min_length" param: %w`, err)
	}

	p = parameters["max_length"]
	if err := p.Scan(&maxLength); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "max_length" param: %w`, err)
	}

	p = parameters["symbols"]
	if err := p.Scan(&symbols); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "symbols" param: %w`, err)
	}

	p = parameters["keep_null"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	if warns := validateRandomStringParams(minLength, maxLength, symbols); warns.IsFatal() {
		return nil, warns, nil
	}

	t, err := transformers.NewRandomStringTransformer([]rune(symbols), minLength, maxLength)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create transformer: %w", err)
	}

	p = parameters["engine"]
	if err := p.Scan(&engine); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "engine" param: %w`, err)
	}

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &RandomStringTransformer{
		t:               t,
		columnName:      columnName,
		keepNull:        keepNull,
		affectedColumns: affectedColumns,
		columnIdx:       idx,
	}, nil, nil
}

func (rst *RandomStringTransformer) GetAffectedColumns() map[int]string {
	return rst.affectedColumns
}

func (rst *RandomStringTransformer) Init(ctx context.Context) error {
	return nil
}

func (rst *RandomStringTransformer) Done(ctx context.Context) error {
	return nil
}

func (rst *RandomStringTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	val, err := r.GetRawColumnValueByIdx(rst.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if val.IsNull && rst.keepNull {
		return r, nil
	}

	newVal, err := rst.t.Transform(val.Data)
	if err != nil {
		return nil, fmt.Errorf("unable to transform value: %w", err)
	}
	res := toolkit.NewRawValue([]byte(string(newVal)), false)

	if err = r.SetRawColumnValueByIdx(rst.columnIdx, res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}

	return r, nil
}

func validateRandomStringParams(minLength, maxLength int, symbols string) toolkit.ValidationWarnings {
	var warns toolkit.ValidationWarnings
	if minLength < 0 {
		warns = append(warns, toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("ParameterName", "min_length").
			AddMeta("ParameterValue", minLength).
			SetMsg("min_length cannot be less than zero"))
	}
	if maxLength < minLength {
		warns = append(warns, toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("ParameterName", "max_length").
			AddMeta("ParameterValue", maxLength).
			AddMeta("MinLength", minLength).
			SetMsg("max_length cannot be less than min_length"))
	}
	if symbols == "" {
		warns = append(warns, toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("ParameterName", "symbols").
			SetMsg("symbols cannot be empty"))
	}
	return warns
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(stringTransformerDefinition)
}
