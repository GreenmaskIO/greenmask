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
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	commonparameters "github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
)

// TransformerContext - supplied transformer and conditions that have to be executed.
type TransformerContext struct {
	Transformer core.Transformer
	// Condition - transformer level condition to evaluate before applying the transformer.
	Condition         core.CondEvaluator
	StaticParameters  map[string]*commonparameters.StaticParameter
	DynamicParameters map[string]*commonparameters.DynamicParameter
}

func (tc *TransformerContext) SetRecordForDynamicParameters(r core.Recorder) {
	for _, param := range tc.DynamicParameters {
		param.SetRecord(r)
	}
}

func (tc *TransformerContext) EvaluateWhen(r core.Recorder) (bool, error) {
	if tc.Condition == nil {
		return true, nil
	}
	return tc.Condition.Evaluate(r)
}

func (tc *TransformerContext) Init(ctx context.Context) error {
	return tc.Transformer.Init(ctx)
}

func (tc *TransformerContext) GetAffectedColumns() map[int]string {
	return tc.Transformer.GetAffectedColumns()
}

func (tc *TransformerContext) Describe() string {
	return tc.Transformer.Describe()
}

// TableDumpContext - everything related to the table that must be applied for a record.
// It contains table, transformers, dump query, table driver and conditions.
type TableDumpContext struct {
	Table              *core.Table
	TransformerContext []core.TransformerContexter
	// Condition - table level condition to evaluate before applying any transformers.
	Condition   core.CondEvaluator
	Query       string
	TableDriver core.TableDriver
}

func (tc *TableDumpContext) HasTransformer() bool {
	return len(tc.TransformerContext) > 0
}

func (tc *TableDumpContext) GetAffectedColumns() []int {
	affectedColumns := make(map[int]struct{})
	for _, transformerCtx := range tc.TransformerContext {
		ac := transformerCtx.GetAffectedColumns()
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
func (tc *TableDumpContext) EvaluateWhen(r core.Recorder) (bool, error) {
	if tc.Condition == nil {
		return true, nil
	}
	return tc.Condition.Evaluate(r)
}

func (tc *TableDumpContext) Init(ctx context.Context) error {
	for i, transformerCtx := range tc.TransformerContext {
		if err := transformerCtx.Init(ctx); err != nil {
			return fmt.Errorf("initialize transformer pos=%d name='%s': %w",
				i, transformerCtx.Describe(), err,
			)
		}
	}
	return nil
}
