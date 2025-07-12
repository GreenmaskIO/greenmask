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

func PrintValidationWarnings(ctx context.Context, vc *validationcollector.Collector, resolvedWarnings []string, printAll bool) error {
	if !vc.HasWarnings() {
		return nil
	}
	for _, w := range vc.GetWarnings() {
		w.MakeHash()
		if idx := slices.Index(resolvedWarnings, w.Hash); idx != -1 {
			log.Ctx(ctx).Debug().Str("hash", w.Hash).Msg("resolved warning has been excluded")
			if w.Severity == commonmodels.ValidationSeverityError {
				return fmt.Errorf(
					"exclude warning %s with hash: %w", w.Hash, errCannotExcludeWarningWithErrorSeverity,
				)
			}
			continue
		}

		if w.Severity == commonmodels.ValidationSeverityWarning {
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
