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

var NoiseFloatTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"NoiseFloat",
		"Make noise float for int",
	),
	NewNoiseFloatTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("float4", "float8", "numeric").
		SetSkipOnNull(true),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"ratio",
		"max random percentage for noise",
	).SetDefaultValue(toolkit.ParamsValue("0.1")),

	toolkit.MustNewParameterDefinition(
		"precision",
		"precision of noised float value (number of digits after coma)",
	).SetDefaultValue(toolkit.ParamsValue("4")),
)

type NoiseFloatTransformerParams struct {
	Ratio     float64 `mapstructure:"ratio" validate:"required,min=0,max=1"`
	Precision int16   `mapstructure:"precision"`
	Nullable  bool    `mapstructure:"nullable"`
	Fraction  float32 `mapstructure:"fraction"`
}

type NoiseFloatTransformer struct {
	columnName      string
	columnIdx       int
	ratio           float64
	precision       int
	rand            *rand.Rand
	affectedColumns map[int]string
}

func NewNoiseFloatTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.ParameterDefinition) (utils.Transformer, toolkit.ValidationWarnings, error) {
	// TODO: value out of rage might be possible: double check this transformer implementation

	var columnName string
	var ratio float64
	var precision int

	p := parameters["column"]
	if _, err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	p = parameters["ratio"]
	if _, err := p.Scan(&ratio); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "ratio" param: %w`, err)
	}

	p = parameters["precision"]
	if _, err := p.Scan(&precision); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "precision" param: %w`, err)
	}

	return &NoiseFloatTransformer{
		precision:       precision,
		ratio:           ratio,
		columnName:      columnName,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		affectedColumns: affectedColumns,
		columnIdx:       idx,
	}, nil, nil
}

func (nft *NoiseFloatTransformer) GetAffectedColumns() map[int]string {
	return nft.affectedColumns
}

func (nft *NoiseFloatTransformer) Init(ctx context.Context) error {
	return nil
}

func (nft *NoiseFloatTransformer) Done(ctx context.Context) error {
	return nil
}

func (nft *NoiseFloatTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	var val float64
	isNull, err := r.ScanColumnValueByIdx(nft.columnIdx, &val)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if isNull {
		return r, nil
	}
	err = r.SetColumnValueByIdx(nft.columnIdx, utils.NoiseFloat(nft.rand, nft.ratio, val, nft.precision))
	if err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(NoiseFloatTransformerDefinition)
}
