// Copyright 2025 Greenmask
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

package context

import (
	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	commonparameters "github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
)

type CondEvaluator interface {
	Evaluate(r interfaces.Recorder) (bool, error)
}

// TransformerContext - supplied transformer and conditions that have to be executed.
type TransformerContext struct {
	Transformer interfaces.Transformer
	// Condition - transformer level condition to evaluate before applying the transformer.
	Condition         CondEvaluator
	StaticParameters  map[string]*commonparameters.DynamicParameter
	DynamicParameters map[string]*commonparameters.DynamicParameter
}

func (tc *TransformerContext) SetRecordForDynamicParameters(r interfaces.Recorder) {
	for _, param := range tc.DynamicParameters {
		param.SetRecord(r)
	}
}

func (tc *TransformerContext) EvaluateWhen(r interfaces.Recorder) (bool, error) {
	if tc.Condition == nil {
		return true, nil
	}
	return tc.Condition.Evaluate(r)
}

// TableContext - everything related to the table that must be applied for a record.
// It contains table, transformers, dump query, table driver and conditions.
type TableContext struct {
	Table              *commonmodels.Table
	TransformerContext []*TransformerContext
	// Condition - table level condition to evaluate before applying any transformers.
	Condition   CondEvaluator
	Query       string
	TableDriver interfaces.TableDriver
}

func (tc *TableContext) HasTransformer() bool {
	return len(tc.TransformerContext) > 0
}

func (tc *TableContext) GetAffectedColumns() []int {
	affectedColumns := make(map[int]struct{})
	for _, transformerCtx := range tc.TransformerContext {
		ac := transformerCtx.Transformer.GetAffectedColumns()
		for idx := range ac {
			affectedColumns[idx] = struct{}{}
		}
	}
	res := make([]int, 0, len(affectedColumns))
	for col := range affectedColumns {
		res = append(res, col)
	}
	return res
}
func (tc *TableContext) EvaluateWhen(r interfaces.Recorder) (bool, error) {
	if tc.Condition == nil {
		return true, nil
	}
	return tc.Condition.Evaluate(r)
}
