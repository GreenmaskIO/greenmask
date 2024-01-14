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

var NoiseIntTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"NoiseInt",
		"Make noise value for int",
	),

	NewNoiseIntTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("int2", "int4", "int8").
		SetSkipOnNull(true),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"ratio",
		"max random percentage for noise",
	).SetRequired(true).
		SetDefaultValue(toolkit.ParamsValue("0.1")),
)

type NoiseIntTransformer struct {
	columnName      string
	columnIdx       int
	ratio           float64
	rand            *rand.Rand
	affectedColumns map[int]string
}

func NewNoiseIntTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	var columnName string
	var ratio float64

	p := parameters["column"]
	if _, err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["ratio"]
	if _, err := p.Scan(&ratio); err != nil {
		return nil, nil, fmt.Errorf("unable to scan type param: %w", err)
	}

	return &NoiseIntTransformer{
		ratio:           ratio,
		columnName:      columnName,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		affectedColumns: affectedColumns,
		columnIdx:       idx,
	}, nil, nil
}

func (nit *NoiseIntTransformer) GetAffectedColumns() map[int]string {
	return nit.affectedColumns
}

func (nit *NoiseIntTransformer) Init(ctx context.Context) error {
	return nil
}

func (nit *NoiseIntTransformer) Done(ctx context.Context) error {
	return nil
}

func (nit *NoiseIntTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	// TODO: value out of rage might be possible: double check this transformer implementation

	var val int64
	isNull, err := r.ScanColumnValueByIdx(nit.columnIdx, &val)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if isNull {
		return r, nil
	}

	if err := r.SetColumnValueByIdx(nit.columnIdx, utils.NoiseInt(nit.rand, nit.ratio, val)); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(NoiseIntTransformerDefinition)
}
