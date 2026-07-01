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

// printCollectedWarnings prints the supplied validation warnings and returns
// core.ErrFatalValidationError when any fatal warning is present.
//
// It takes the warnings as a plain slice (carried on the run's serialisable
// state) rather than reading a collector from ctx — the collector is an
// internal pipeline detail. cfg.Validate.Warnings governs non-fatal verbosity
// and cfg.Validate.ResolvedWarnings the acknowledged-hash allowlist;
// error-severity warnings are always printed and can never be suppressed. ctx
// is used only for logging.
func printCollectedWarnings(ctx context.Context, warnings core.ValidationWarnings, cfg *config.Config) error {
	if err := commonutils.PrintValidationWarningsList(
		ctx, warnings, cfg.Validate.ResolvedWarnings, cfg.Validate.Warnings,
	); err != nil {
		return fmt.Errorf("print validation warnings: %w", err)
	}
	if warnings.IsFatal() {
		return core.ErrFatalValidationError
	}
	return nil
}
