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
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

const SetNullTransformerName = "SetNull"

var SetNullTransformerDefinition = transformerutils.NewTransformerDefinition(
	transformerutils.NewTransformerProperties(
		SetNullTransformerName,
		"Set NULL value",
	),

	NewSetNullTransformer,

	commonparameters.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(commonparameters.NewColumnProperties().
		SetAffected(true).
		SetNullable(true),
	).SetRequired(true),
)

type SetNullTransformer struct {
	columnName      string
	columnIdx       int
	affectedColumns map[int]string
}

func NewSetNullTransformer(
	ctx context.Context,
	tableDriver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error) {
	columnName, column, err := getColumnParameterValue(ctx, tableDriver, parameters)
	if err != nil {
		return nil, fmt.Errorf("get \"column\" parameter: %w", err)
	}

	return &SetNullTransformer{
		columnName: columnName,
		columnIdx:  column.Idx,
		affectedColumns: map[int]string{
			column.Idx: columnName,
		},
	}, nil
}

func (sut *SetNullTransformer) GetAffectedColumns() map[int]string {
	return sut.affectedColumns
}

func (sut *SetNullTransformer) Init(context.Context) error {
	return nil
}

func (sut *SetNullTransformer) Done(context.Context) error {
	return nil
}

func (sut *SetNullTransformer) Transform(_ context.Context, r commonininterfaces.Recorder) error {
	if err := r.SetRawColumnValueByIdx(sut.columnIdx, commonmodels.NewColumnRawValue(nil, true)); err != nil {
		return fmt.Errorf("unable to set new value: %w", err)
	}
	return nil
}
