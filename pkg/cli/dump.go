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
	"github.com/greenmaskio/greenmask/pkg/common/dump/pipeline"
	"github.com/greenmaskio/greenmask/pkg/engines"
)

func (g *Cli) Dump(ctx context.Context) error {
	if err := g.initInfrastructure(); err != nil {
		return fmt.Errorf("setup infrastructure: %w", err)
	}
	ctx = SetupContext(ctx, g.cfg)
	p, err := engines.NewDumpPipeline(g.cfg)
	if err != nil {
		return fmt.Errorf("create dump pipeline: %w", err)
	}
	state, runErr := p.RunDump(ctx, *g.cfg)
	// The pipeline records collected warnings on the returned state (populated
	// even when runErr is non-nil). Print them first — a fatal warning is usually
	// the cause of runErr — then decide the return code: fatal warnings take
	// precedence, otherwise propagate the wrapped run error.
	if printErr := printCollectedWarnings(ctx, runStateWarnings(state), g.cfg); printErr != nil {
		return fmt.Errorf("print collected warnings: %w", printErr)
	}
	if runErr != nil {
		return fmt.Errorf("run dump: %w", runErr)
	}
	return nil
}

// runStateWarnings safely extracts the collected warnings from a RunState that
// may be nil (RunDump returns a *RunState, so a nil is possible even though the
// pipeline populates it before any error can occur).
func runStateWarnings(state *pipeline.RunState) core.ValidationWarnings {
	if state == nil {
		return nil
	}
	return state.Warnings
}
