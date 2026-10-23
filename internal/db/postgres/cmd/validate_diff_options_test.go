package cmd

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/greenmaskio/greenmask/internal/domains"
)

func TestValidateDiffOptions(t *testing.T) {
	tests := []struct {
		name    string
		cfg     domains.Validate
		wantErr string
	}{
		{name: "default values mode", cfg: domains.Validate{DiffMode: ValuesDiffMode, DiffUnchangedThreshold: 50}},
		{name: "summary with diff", cfg: domains.Validate{Diff: true, DiffMode: SummaryDiffMode, DiffUnchangedThreshold: 50}},
		{name: "threshold 0 and 100 are allowed", cfg: domains.Validate{Diff: true, DiffMode: SummaryDiffMode, DiffUnchangedThreshold: 100}},
		{name: "summary needs diff", cfg: domains.Validate{DiffMode: SummaryDiffMode, DiffUnchangedThreshold: 50}, wantErr: "requires --diff"},
		{name: "unknown mode", cfg: domains.Validate{DiffMode: "SUMMARY", DiffUnchangedThreshold: 50}, wantErr: "unknown --diff-mode"},
		{name: "unset mode means values", cfg: domains.Validate{DiffUnchangedThreshold: 50}},
		{name: "negative threshold", cfg: domains.Validate{DiffMode: ValuesDiffMode, DiffUnchangedThreshold: -0.1}, wantErr: "between 0 and 100"},
		{name: "threshold over 100", cfg: domains.Validate{DiffMode: ValuesDiffMode, DiffUnchangedThreshold: 100.1}, wantErr: "between 0 and 100"},
		{name: "NaN threshold", cfg: domains.Validate{DiffMode: ValuesDiffMode, DiffUnchangedThreshold: math.NaN()}, wantErr: "between 0 and 100"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDiffOptions(&tt.cfg)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
