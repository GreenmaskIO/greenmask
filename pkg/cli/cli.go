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
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/config"
)

// Cli implements engines.Utility for the CLI and for programmatic
// callers (e.g. gm-backend). Infrastructure (logger, engine validation) and
// storage are initialised lazily on the first method call that needs them,
// so constructing a Cli is always cheap and free of side-effects.
//
// CLI-specific parameters that cannot live in the shared config (delete
// policy, list-dumps filtering, output formats) are supplied via fluent
// For* methods before calling the operation. Each command owns its For*
// method and operation method in its own file.
type Cli struct {
	cfg *config.Config
	st  core.Storager // lazily initialised; shared across calls

	// CLI operation parameters — set via For* methods.
	deleteOpts             *DeleteOptions
	listDumpsQuiet         bool
	listDumpsFormat        OutputFormat
	listDumpsFilter        *Filter
	listTransformersFormat OutputFormat
	showTransformerFormat  OutputFormat
}

// New returns a Cli initialised with the given config. No IO is
// performed; call For* methods to supply operation-specific parameters,
// then call the desired operation method.
func New(cfg *config.Config) *Cli {
	return &Cli{cfg: cfg}
}

// initInfrastructure validates config and sets up the logger. Idempotent.
func (g *Cli) initInfrastructure() error {
	return SetupInfrastructure(g.cfg)
}

// storage returns the cached Storager, creating it on the first call.
func (g *Cli) storage(ctx context.Context) (core.Storager, error) {
	if g.st != nil {
		return g.st, nil
	}
	st, err := commonutils.GetStorage(ctx, g.cfg)
	if err != nil {
		return nil, fmt.Errorf("init storage: %w", err)
	}
	g.st = st
	return st, nil
}
