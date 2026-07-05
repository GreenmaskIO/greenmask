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

package cli

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	restorepipeline "github.com/greenmaskio/greenmask/pkg/common/restore/pipeline"
	"github.com/greenmaskio/greenmask/pkg/engines"
)

func (g *Cli) Restore(ctx context.Context, dumpID string) error {
	if err := g.initInfrastructure(); err != nil {
		return fmt.Errorf("setup infrastructure: %w", err)
	}
	ctx = SetupContext(ctx, g.cfg)
	parsedDumpID := core.DumpID(dumpID)
	if err := parsedDumpID.Validate(); err != nil {
		return fmt.Errorf("validate dumpID: %w", err)
	}
	p, err := engines.NewRestorePipeline(g.cfg)
	if err != nil {
		return fmt.Errorf("create restore pipeline: %w", err)
	}
	state, runErr := p.RunRestore(ctx, *g.cfg, parsedDumpID)
	// The pipeline records collected warnings on the returned state (populated
	// even when runErr is non-nil). Print them first — a fatal warning is usually
	// the cause of runErr — then decide the return code: fatal warnings take
	// precedence, otherwise propagate the wrapped run error.
	if printErr := printCollectedWarnings(ctx, restoreStateWarnings(state), g.cfg); printErr != nil {
		return fmt.Errorf("print collected warnings: %w", printErr)
	}
	if runErr != nil {
		return fmt.Errorf("run restore: %w", runErr)
	}
	return nil
}

// restoreStateWarnings safely extracts the collected warnings from a
// RestoreRunState that may be nil (RunRestore returns a *RestoreRunState, so a
// nil is possible even though the pipeline populates it before any error).
func restoreStateWarnings(state *restorepipeline.RestoreRunState) core.ValidationWarnings {
	if state == nil {
		return nil
	}
	return state.Warnings
}
