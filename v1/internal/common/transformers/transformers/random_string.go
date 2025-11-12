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

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/generators/transformers"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

const TransformerNameRandomString = "RandomString"

var RandomStringTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		TransformerNameRandomString,
		"Generate a string withing the specified length with provided char set",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewRandomStringTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		commonparameters.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypeClasses(commonmodels.TypeClassText),
	).SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"min_length",
		"min length of string",
	).SetSupportTemplate(true).
		SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"max_length",
		"max length of string",
	).SetSupportTemplate(true).
		SetRequired(true),

	commonparameters.MustNewParameterDefinition(
		"symbols",
		"the characters range for random string",
	).SetDefaultValue(commonmodels.ParamsValue("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")),

	commonparameters.MustNewParameterDefinition(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(commonmodels.ParamsValue("true")),

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
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
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

func (t *RandomStringTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	val, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if val.IsNull && t.keepNull {
		return nil
	}

	res := commonmodels.NewColumnRawValue(
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
