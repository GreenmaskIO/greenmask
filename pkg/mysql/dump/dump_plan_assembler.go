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

package dump

import (
	"context"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var _ core.DumpPlanAssembler = (*DumpPlanAssembler)(nil)

// DumpPlanAssembler combines runtime artifacts into the executable dump plan.
type DumpPlanAssembler struct{}

func (s *DumpPlanAssembler) Assemble(ctx context.Context, input core.DumpPlanInput) (core.DumpPlan, error) {
	databases := make([]string, 0, len(input.IntrospectionResult.KindsMap[core.ObjectKindMysqlDatabase]))
	for _, obj := range input.IntrospectionResult.KindsMap[core.ObjectKindMysqlDatabase] {
		databases = append(databases, obj.Name)
	}
	return core.DumpPlan{
		DumpObjectSpecs:      input.DumpContext.DumpObjectSpecs,
		SchemaDumpSpecs:      input.DumpContext.SchemaDumpSpecs,
		RestorationContext:   input.RestorationContext,
		TransformationConfig: input.Config,
		IntrospectionResult:  input.IntrospectionResult,
		MatchedDatabases:     databases,
		Tags:                 input.Tags,
		Description:          input.Description,
	}, nil
}
