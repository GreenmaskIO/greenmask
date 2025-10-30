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
	"github.com/greenmaskio/greenmask/v1/internal/common/conditions"
	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

// TransformerContext - supplied transformer and conditions that have to be executed.
type TransformerContext struct {
	Transformer commonininterfaces.Transformer
	WhenCond    *conditions.WhenCond
}

func (tc *TransformerContext) EvaluateWhen(r commonininterfaces.Recorder) (bool, error) {
	if tc.WhenCond == nil {
		return true, nil
	}
	return tc.WhenCond.Evaluate(r)
}

// TableContext - everything related to the table that must be applied for a record.
// It contains table, transformers, dump query, table driver and conditions.
type TableContext struct {
	Table              *commonmodels.Table
	TransformerContext []*TransformerContext
	TableCondition     *conditions.WhenCond
	Query              string
	TableDriver        commonininterfaces.TableDriver
}

func (tc *TableContext) HasTransformer() bool {
	return len(tc.TransformerContext) > 0
}
