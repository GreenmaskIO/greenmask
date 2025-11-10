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

const TransformerNameRandomBool = "RandomBool"

var BoolTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		TransformerNameRandomBool,
		"Generate random bool",
	).AddMeta(transformerutils.AllowApplyForReferenced, true).
		AddMeta(transformerutils.RequireHashEngineParameter, true),

	NewBooleanTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypeClasses(commonmodels.TypeClassBoolean),
	).SetRequired(true),

	defaultKeepNullParameterDefinition,

	defaultEngineParameterDefinition,
)

type BooleanTransformer struct {
	columnName      string
	keepNull        bool
	affectedColumns map[int]string
	columnIdx       int
	t               *transformers.RandomBoolean
}

func NewBooleanTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
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

	t := transformers.NewRandomBoolean()
	g, err := getGenerateEngine(ctx, engine, t.GetRequiredGeneratorByteLength())
	if err != nil {
		return nil, fmt.Errorf("unable to get generator: %w", err)
	}
	if err = t.SetGenerator(g); err != nil {
		return nil, fmt.Errorf("unable to set generator: %w", err)
	}

	return &BooleanTransformer{
		columnName: columnName,
		keepNull:   keepNull,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
		columnIdx: column.Idx,
		t:         t,
	}, nil
}

func (t *BooleanTransformer) GetAffectedColumns() map[int]string {
	return t.affectedColumns
}

func (t *BooleanTransformer) Init(context.Context) error {
	return nil
}

func (t *BooleanTransformer) Done(context.Context) error {
	return nil
}

func (t *BooleanTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	val, err := r.GetRawColumnValueByIdx(t.columnIdx)
	if err != nil {
		return fmt.Errorf("scan value: %w", err)
	}
	if val.IsNull && t.keepNull {
		return nil
	}

	boolVal, err := t.t.Transform(val.Data)
	if err != nil {
		return fmt.Errorf("transform value: %w", err)
	}

	if err = r.SetColumnValueByIdx(t.columnIdx, boolVal); err != nil {
		return fmt.Errorf("set new value: %w", err)
	}
	return nil
}

func (t *BooleanTransformer) Describe() string {
	return TransformerNameRandomBool
}
