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

package core

import "context"

// RestorePlan is the finalized execution plan derived from Metadata.
//
// It mirrors DumpPlan: ObjectRestoreSpecs carry typed Payload fields built by
// the engine-specific RestorePlanBuilder from persisted RestorationItem JSON.
type RestorePlan struct {
	ObjectRestoreSpecs []ObjectRestoreSpec
	SchemaRestoreSpecs []SchemaRestoreSpec
	RestorationContext RestorationContext
}

// RestorePlanBuilder converts Metadata into a RestorePlan with typed Payload
// fields ready for factory consumption.
//
// It mirrors DumpPlanAssembler: each engine registers its own implementation
// in RestoreStages. The builder runs between metadata reading and processor
// execution in restore_pipeline.go Execute().
type RestorePlanBuilder interface {
	Build(ctx context.Context, meta Metadata) (RestorePlan, error)
}
