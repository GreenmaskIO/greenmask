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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
)

func TestPrintCollectedWarnings(t *testing.T) {
	// hashOf mirrors how the printer derives a warning's hash so a test case can
	// pre-acknowledge it via ResolvedWarnings.
	hashOf := func(w *core.ValidationWarning) string {
		w.MakeHash()
		return w.Hash
	}

	nonFatal := func() *core.ValidationWarning {
		return core.NewValidationWarning().
			SetSeverity(core.ValidationSeverityWarning).
			SetMsg("non-fatal warning")
	}
	fatal := func() *core.ValidationWarning {
		return core.NewValidationWarning().
			SetSeverity(core.ValidationSeverityError).
			SetMsg("fatal warning")
	}

	tests := []struct {
		name             string
		warnings         []*core.ValidationWarning
		printAll         bool
		resolvedWarnings func([]*core.ValidationWarning) []string
		wantErr          error
	}{
		{
			name:     "no warnings",
			warnings: nil,
			wantErr:  nil,
		},
		{
			name:     "non-fatal only, warnings disabled",
			warnings: []*core.ValidationWarning{nonFatal()},
			printAll: false,
			wantErr:  nil,
		},
		{
			name:     "non-fatal only, warnings enabled",
			warnings: []*core.ValidationWarning{nonFatal()},
			printAll: true,
			wantErr:  nil,
		},
		{
			name:     "fatal present",
			warnings: []*core.ValidationWarning{fatal()},
			printAll: false,
			wantErr:  core.ErrFatalValidationError,
		},
		{
			name:     "non-fatal suppressed by resolved hash",
			warnings: []*core.ValidationWarning{nonFatal()},
			printAll: true,
			resolvedWarnings: func(ws []*core.ValidationWarning) []string {
				return []string{hashOf(ws[0])}
			},
			wantErr: nil,
		},
		{
			name:     "error severity refuses suppression",
			warnings: []*core.ValidationWarning{fatal()},
			printAll: false,
			resolvedWarnings: func(ws []*core.ValidationWarning) []string {
				return []string{hashOf(ws[0])}
			},
			// PrintValidationWarnings returns the wrapped
			// errCannotExcludeWarningWithErrorSeverity error, surfaced through
			// printCollectedWarnings.
			wantErr: nil, // asserted separately below
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Validate.Warnings = tc.printAll
			if tc.resolvedWarnings != nil {
				cfg.Validate.ResolvedWarnings = tc.resolvedWarnings(tc.warnings)
			}

			err := printCollectedWarnings(context.Background(), tc.warnings, cfg)

			if tc.name == "error severity refuses suppression" {
				// A resolved hash on an error-severity warning is rejected by
				// the printer; the error is wrapped, not the fatal sentinel.
				require.Error(t, err)
				assert.NotErrorIs(t, err, core.ErrFatalValidationError)
				return
			}

			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
