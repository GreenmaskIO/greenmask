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

var RandomFloatTransformerDefinition = utils.NewTransformerDefinition(
	utils.NewTransformerProperties(
		"RandomFloat",
		"Generate random float",
	),

	NewRandomFloatTransformer,

	toolkit.MustNewParameterDefinition(
		"column",
		"column name",
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("float4", "float8", "numeric"),
	).SetRequired(true),

	toolkit.MustNewParameterDefinition(
		"min",
		"min float threshold of random value. The value range depends on column type",
	).SetRequired(true).
		SetLinkParameter("column").
		SetDynamicModeSupport(true),

	toolkit.MustNewParameterDefinition(
		"max",
		"min float threshold of random value. The value range depends on column type",
	).SetRequired(true).
		SetLinkParameter("column").
		SetDynamicModeSupport(true),

	toolkit.MustNewParameterDefinition(
		"precision",
		"precision of random float value (number of digits after coma)",
	).SetDefaultValue(toolkit.ParamsValue("4")),

	toolkit.MustNewParameterDefinition(
		"keep_null",
		"indicates that NULL values must not be replaced with transformed values",
	).SetDefaultValue(toolkit.ParamsValue("true")),
)

type RandomFloatTransformer struct {
	columnName      string
	keepNull        bool
	precision       int
	rand            *rand.Rand
	affectedColumns map[int]string
	columnIdx       int
	minVal          float64
	maxVal          float64

	columnParam    toolkit.Parameterizer
	maxParam       toolkit.Parameterizer
	minParam       toolkit.Parameterizer
	precisionParam toolkit.Parameterizer
	keepNullParam  toolkit.Parameterizer
}

func NewRandomFloatTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (utils.Transformer, toolkit.ValidationWarnings, error) {
	columnParam := parameters["column"]
	minParam := parameters["min"]
	maxParam := parameters["max"]
	precisionParam := parameters["precision"]
	keepNullParam := parameters["keep_null"]

	var columnName string
	var precision int
	var keepNull bool

	if err := columnParam.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

	if err := precisionParam.Scan(&precision); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "precision" param: %w`, err)
	}

	if err := keepNullParam.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keep_null" param: %w`, err)
	}

	return &RandomFloatTransformer{
		keepNull:        keepNull,
		precision:       precision,
		columnName:      columnName,
		rand:            rand.New(rand.NewSource(time.Now().UnixMicro())),
		affectedColumns: affectedColumns,
		columnIdx:       idx,

		columnParam:    columnParam,
		minParam:       minParam,
		maxParam:       maxParam,
		precisionParam: precisionParam,
		keepNullParam:  keepNullParam,
	}, nil, nil

}

func (rft *RandomFloatTransformer) GetAffectedColumns() map[int]string {
	return rft.affectedColumns
}

func (rft *RandomFloatTransformer) Init(ctx context.Context) error {
	return nil
}

func (rft *RandomFloatTransformer) Done(ctx context.Context) error {
	return nil
}

func (rft *RandomFloatTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	valAny, err := r.GetRawColumnValueByIdx(rft.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if valAny.IsNull && rft.keepNull {
		return r, nil
	}

	var minVal, maxVal float64
	err = rft.minParam.Scan(&minVal)
	if err != nil {
		return nil, fmt.Errorf(`error getting "min" parameter value: %w`, err)
	}

	err = rft.maxParam.Scan(&maxVal)
	if err != nil {
		return nil, fmt.Errorf(`error getting "max" parameter value: %w`, err)
	}

	if minVal >= maxVal {
		return nil, fmt.Errorf("max value must be greater than min: got min = %f max = %f", minVal, maxVal)
	}

	err = r.SetColumnValueByIdx(rft.columnIdx, toolkit.RandomFloat(rft.rand, minVal, maxVal, rft.precision))
	if err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomFloatTransformerDefinition)
}
