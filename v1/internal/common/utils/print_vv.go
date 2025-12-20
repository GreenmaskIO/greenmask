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

	"github.com/rs/zerolog/log"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

var (
	errCannotExcludeWarningWithErrorSeverity = errors.New("cannot exclude warnings with errors severity")
)

func PrintValidationWarnings(ctx context.Context, resolvedWarnings []string, printAll bool) error {
	vc := validationcollector.FromContext(ctx)
	if !vc.HasWarnings() {
		return nil
	}
	for _, w := range vc.GetWarnings() {
		w.MakeHash()
		if idx := slices.Index(resolvedWarnings, w.Hash); idx != -1 {
			log.Ctx(ctx).Debug().
				Str("Hash", w.Hash).
				Msg("resolved warning has been excluded")
			if w.Severity == commonmodels.ValidationSeverityError {
				return fmt.Errorf(
					"exclude warning %s with hash: %w", w.Hash, errCannotExcludeWarningWithErrorSeverity,
				)
			}
			continue
		}

		if w.Severity == commonmodels.ValidationSeverityError {
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
