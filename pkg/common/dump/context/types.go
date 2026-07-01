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
	core "github.com/greenmaskio/greenmask/pkg/common/core"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
)

// TransformerContext is an alias for the shared transformer context type.
type TransformerContext = transformercontext.TransformerContext

// TableDumpContextPayload holds everything needed to dump and transform one table.
type TableDumpContextPayload struct {
	Table              *core.Table
	TransformerContext []*TransformerContext
	Condition          core.CondEvaluator
	Query              string
	TableDriver        core.TableDriver
	// ColumnKind is the engine-specific kind used for column attribute
	// identities in the snapshot (e.g. core.EntityKindMysqlColumn).
	ColumnKind core.EntityKind
}

func (tc *TableDumpContextPayload) HasTransformer() bool {
	return len(tc.TransformerContext) > 0
}

func (tc *TableDumpContextPayload) EvaluateWhen(r core.Recorder) (bool, error) {
	if tc.Condition == nil {
		return true, nil
	}
	return tc.Condition.Evaluate(r)
}

func (tc *TableDumpContextPayload) GetAffectedColumns() []int {
	seen := make(map[int]struct{})
	for _, t := range tc.TransformerContext {
		for idx := range t.GetAffectedColumns() {
			seen[idx] = struct{}{}
		}
	}
	res := make([]int, 0, len(seen))
	for col := range seen {
		res = append(res, col)
	}
	return res
}

// GetSnapshot builds the engine-agnostic portion of an ObjectSnapshot from this
// payload's runtime context. See transformercontext.BuildObjectSnapshot.
func (tc *TableDumpContextPayload) GetSnapshot() (core.ObjectSnapshot, error) {
	transformers := make([]core.TransformerContexter, len(tc.TransformerContext))
	for i, t := range tc.TransformerContext {
		transformers[i] = t
	}
	return transformercontext.BuildObjectSnapshot(tc.Table, tc.Query, tc.Condition, transformers, tc.ColumnKind)
}
