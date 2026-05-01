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

package restore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/config"
)

func newRestoreWithRemap(remap map[string]string, mode models.DatabaseReplacementMode) *Restore {
	cfg := &config.Config{}
	cfg.Restore.Options.RemapDatabase = remap
	cfg.Restore.Options.DatabaseReplaceMode = mode
	return &Restore{cfg: cfg}
}

func TestRestore_remapDB(t *testing.T) {
	tests := []struct {
		name        string
		remap       map[string]string
		mode        models.DatabaseReplacementMode
		databases   []string
		wantRemap   map[string]string
		wantErr     bool
		errContains string
	}{
		{
			name:      "nil remap returns nil regardless of mode",
			remap:     nil,
			mode:      models.DatabaseReplaceModeStrict,
			databases: []string{"mydb"},
			wantRemap: nil,
		},
		{
			name:      "empty remap returns nil regardless of mode",
			remap:     map[string]string{},
			mode:      models.DatabaseReplaceModeStrict,
			databases: []string{"mydb"},
			wantRemap: nil,
		},
		{
			name:      "strict mode — all databases mapped — returns remap",
			remap:     map[string]string{"src": "dst", "src2": "dst2"},
			mode:      models.DatabaseReplaceModeStrict,
			databases: []string{"src", "src2"},
			wantRemap: map[string]string{"src": "dst", "src2": "dst2"},
		},
		{
			name:        "strict mode — unmapped database — error",
			remap:       map[string]string{"src": "dst"},
			mode:        models.DatabaseReplaceModeStrict,
			databases:   []string{"src", "unmapped"},
			wantErr:     true,
			errContains: "unmapped",
		},
		{
			name:        "empty mode defaults to strict — unmapped database — error",
			remap:       map[string]string{"src": "dst"},
			mode:        "",
			databases:   []string{"src", "other"},
			wantErr:     true,
			errContains: "other",
		},
		{
			name:      "relaxed mode — unmapped database — no error",
			remap:     map[string]string{"src": "dst"},
			mode:      models.DatabaseReplaceModeRelaxed,
			databases: []string{"src", "other"},
			wantRemap: map[string]string{"src": "dst"},
		},
		{
			name:      "relaxed mode — no databases in dump — no error",
			remap:     map[string]string{"src": "dst"},
			mode:      models.DatabaseReplaceModeRelaxed,
			databases: nil,
			wantRemap: map[string]string{"src": "dst"},
		},
		{
			name:        "unknown mode — error",
			remap:       map[string]string{"src": "dst"},
			mode:        "bogus",
			databases:   []string{"src"},
			wantErr:     true,
			errContains: "bogus",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newRestoreWithRemap(tc.remap, tc.mode)
			meta := models.Metadata{Databases: tc.databases}
			got, err := r.remapDB(meta)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.wantRemap, got)
			}
		})
	}
}
