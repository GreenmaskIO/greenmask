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

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/utils"
)

const TransformerNameRandomString = "RandomString"

var RandomStringTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		TransformerNameRandomString,
		"Generate a string withing the specified length with provided char set",
	).AddMeta(utils.AllowApplyForReferenced, true).
		AddMeta(utils.RequireHashEngineParameter, true),

	NewRandomStringTransformer,

	parameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		models.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypeClasses(models.TypeClassText),
	).SetRequired(true),

	parameters.MustNewParameterDefinition(
		"min_length",
		"min length of string",
	).SetSupportTemplate(true).
		SetRequired(true),

	parameters.MustNewParameterDefinition(
		"max_length",
		"max length of string",
	).SetSupportTemplate(true).
		SetRequired(true),

	parameters.MustNewParameterDefinition(
		"symbols",
		"the characters range for random string",
	).SetDefaultValue(models.ParamsValue("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")),

	parameters.MustNewParameterDefinition(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(models.ParamsValue("true")),

	defaultKeepNullParameterDefinition,

	defaultEngineParameterDefinition,
)

type RandomStringTransformer struct {
	t               *transformers.RandomStringTransformer
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
	columnIdx       int
}

func NewRandomStringTransformer(
	ctx context.Context,
	tableDriver interfaces.TableDriver,
	parameters map[string]parameters.Parameterizer,
) (interfaces.Transformer, error) {
	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	minLength, err := getParameterValueWithName[int](ctx, parameters, "min_length")
	if err != nil {
		return nil, fmt.Errorf("get \"min_length\" param: %w", err)
	}

	maxLength, err := getParameterValueWithName[int](ctx, parameters, "max_length")
	if err != nil {
		return nil, fmt.Errorf("get \"min_length\" param: %w", err)
	}

	symbols, err := getParameterValueWithName[string](ctx, parameters, "symbols")
	if err != nil {
		return nil, fmt.Errorf("get \"min_length\" param: %w", err)
	}

	engine, err := getParameterValueWithName[string](ctx, parameters, ParameterNameEngine)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	keepNull, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameKeepNull)
	if err != nil {
		return nil, fmt.Errorf("get \"keep_null\" param: %w", err)
	}

	t, err := transformers.NewRandomStringTransformer([]rune(symbols), minLength, maxLength)
	if err != nil {
		return nil, fmt.Errorf("create transformer: %w", err)
	}

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("set generator: %w", err)
	}

	return &RandomStringTransformer{
		t:          t,
		columnName: columnName,
		keepNull:   keepNull,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx: column.Idx,
	}, nil
}

func (t *RandomStringTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *RandomStringTransformer) Init(context.Context) error {
	return nil
}

func (t *RandomStringTransformer) Done(context.Context) error {
	return nil
}

func (t *RandomStringTransformer) Transform(_ context.Context, r interfaces.Recorder) error {
	val, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if val.IsNull && t.keepNull {
		return nil
	}

	res := models.NewColumnRawValue(
		[]byte(string(t.t.Transform(val.Data))),
		false,
	)

	if err = r.SetRawColumnValueByIdx(t.columnIdx, res); err != nil {
		return fmt.Errorf("set new value: %w", err)
	}

	return nil
}

func (t *RandomStringTransformer) Describe() string {
	return TransformerNameRandomString
}
