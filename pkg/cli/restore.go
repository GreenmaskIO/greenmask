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

	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/engines"
)

func (g *Cli) Restore(ctx context.Context, dumpID string) error {
	if err := g.initInfrastructure(); err != nil {
		return fmt.Errorf("setup infrastructure: %w", err)
	}
	ctx = SetupContext(ctx, g.cfg)
	st, err := g.storage(ctx)
	if err != nil {
		return err
	}
	parsedDumpID := models.DumpID(dumpID)
	if err := parsedDumpID.Validate(); err != nil {
		return fmt.Errorf("validate dumpID: %w", err)
	}
	restorer, err := engines.NewRestorer(g.cfg, st, parsedDumpID)
	if err != nil {
		return fmt.Errorf("create restorer: %w", err)
	}
	return restorer.Run(ctx)
}
