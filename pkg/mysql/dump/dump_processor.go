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
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/dump/processor"
	"github.com/greenmaskio/greenmask/pkg/mysql/dump/factory"
)

var _ core.DumpProcessor = (*DumpProcessor)(nil)

// DumpProcessor executes the final dump plan against the live session.
//
// Runtime resources (session, storage) are injected at execution time via Run;
// it delegates the actual fan-out to the engine-agnostic DefaultDumpProcessorV2.
type DumpProcessor struct{}

func (s *DumpProcessor) Run(
	ctx context.Context,
	session core.DumpSession,
	conn core.ConnectionConfigurer,
	st core.Storager,
	plan core.DumpPlan,
	instruction core.DumpInstruction,
) (core.Metadata, error) {
	objectRegistry, err := factory.NewObjectDumpRegistry()
	if err != nil {
		return core.Metadata{}, fmt.Errorf("build object dump registry: %w", err)
	}
	schemaRegistry, err := factory.NewSchemaDumpRegistry()
	if err != nil {
		return core.Metadata{}, fmt.Errorf("build schema dump registry: %w", err)
	}

	opts := []processor.OptionV2{}
	if instruction.Jobs > 0 {
		opts = append(opts, processor.WithJobsV2(instruction.Jobs))
	}
	proc, err := processor.NewDataDumpProcessorV2(
		objectRegistry,
		schemaRegistry,
		core.DBMSEngineMySQL,
		opts...,
	)
	if err != nil {
		return core.Metadata{}, fmt.Errorf("build dump processor: %w", err)
	}
	return proc.Run(ctx, session, conn, st, plan, instruction)
}
