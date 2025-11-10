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

	"github.com/greenmaskio/greenmask/internal/generators/transformers"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

const TransformerNameRandomUUID = "RandomUuid"

var UUIDTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		TransformerNameRandomUUID,
		"Generate UUID",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewRandomUuidTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(
		commonparameters.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypeClasses(
				commonmodels.TypeClassText,
				commonmodels.TypeClassUuid,
			),
	).SetRequired(true),

	defaultKeepNullParameterDefinition,

	defaultEngineParameterDefinition,
)

type RandomUuidTransformer struct {
	t               *transformers.RandomUuidTransformer
	columnName      string
	columnIdx       int
	keepNull        bool
	affectedColumns map[int]string
}

func NewRandomUuidTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	keepNull, err := getParameterValueWithName[bool](ctx, parameters, ParameterNameKeepNull)
	if err != nil {
		return nil, fmt.Errorf("get \"keep_null\" param: %w", err)
	}

	engine, err := getParameterValueWithName[string](ctx, parameters, ParameterNameEngine)
	if err != nil {
		return nil, fmt.Errorf("get \"engine\" param: %w", err)
	}

	t := transformers.NewRandomUuidTransformer()

	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("set generator: %w", err)
	}

	return &RandomUuidTransformer{
		t:          t,
		columnName: columnName,
		keepNull:   keepNull,
		affectedColumns: map[int]string{
			column.Idx: column.Name,
		},
		columnIdx: column.Idx,
	}, nil
}

func (t *RandomUuidTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *RandomUuidTransformer) Init(context.Context) error {
	return nil
}

func (t *RandomUuidTransformer) Done(context.Context) error {
	return nil
}

func (t *RandomUuidTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	val, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if val.IsNull && t.keepNull {
		return nil
	}

	uuidVal, err := t.t.Transform(val.Data)
	if err != nil {
		return err
	}

	data, err := uuidVal.MarshalText()
	if err != nil {
		return fmt.Errorf("error unmarshal uuid: %w", err)
	}
	if err = r.SetRawColumnValueByIdx(t.columnIdx, commonmodels.NewColumnRawValue(data, false)); err != nil {
		return fmt.Errorf("set new value: %w", err)
	}
	return nil
}

func (t *RandomUuidTransformer) Describe() string {
	return TransformerNameRandomUUID
}
