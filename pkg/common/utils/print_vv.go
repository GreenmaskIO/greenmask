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

package utils

import (
	"context"
	"errors"
	"fmt"
	"slices"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/rs/zerolog/log"
)

var (
	errCannotExcludeWarningWithErrorSeverity = errors.New("cannot exclude warnings with errors severity")
)

// PrintValidationWarnings prints the warnings collected in the ctx collector.
// It is a thin wrapper over PrintValidationWarningsList for callers whose
// warnings still live in the context collector (e.g. the validate pipeline).
func PrintValidationWarnings(ctx context.Context, resolvedWarnings []string, printAll bool) error {
	return PrintValidationWarningsList(ctx, validationcollector.FromContext(ctx).GetWarnings(), resolvedWarnings, printAll)
}

// PrintValidationWarningsList prints the supplied warnings, honouring the
// resolved-hash allowlist and the printAll (non-fatal verbosity) flag. Error
// severity is always printed and can never be excluded. ctx is used only for
// logging. This slice-based form lets callers print warnings carried on a
// serialisable RunState without depending on the collector.
func PrintValidationWarningsList(
	ctx context.Context,
	warnings core.ValidationWarnings,
	resolvedWarnings []string,
	printAll bool,
) error {
	if !warnings.HasWarnings() {
		return nil
	}
	for _, w := range warnings {
		w.MakeHash()
		if idx := slices.Index(resolvedWarnings, w.Hash); idx != -1 {
			log.Ctx(ctx).Debug().
				Str("Hash", w.Hash).
				Msg("resolved warning has been excluded")
			if w.Severity == core.ValidationSeverityError {
				return fmt.Errorf(
					"exclude warning %s with hash: %w", w.Hash, errCannotExcludeWarningWithErrorSeverity,
				)
			}
			continue
		}

		if w.Severity == core.ValidationSeverityError {
			// The warnings with error severity must be printed anyway
			log.Error().Any("ValidationWarning", w).Msg("")
		} else {
			// Print warnings with severity level lower than ValidationSeverityError only if requested
			if printAll {
				log.Warn().Any("ValidationWarning", w).Msg("")
			}
		}
	}
	return nil
}
