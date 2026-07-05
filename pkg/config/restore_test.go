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

package config

import (
	"strings"
	"testing"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

func TestCommonRestoreOptions_Validate(t *testing.T) {
	tests := []struct {
		name        string
		opts        CommonRestoreOptions
		wantErr     bool
		errContains string
	}{
		{
			name: "empty mode is valid (field omitted from config)",
			opts: CommonRestoreOptions{DatabaseReplaceMode: ""},
		},
		{
			name: "strict mode is valid",
			opts: CommonRestoreOptions{DatabaseReplaceMode: core.DatabaseReplaceModeStrict},
		},
		{
			name: "relaxed mode is valid",
			opts: CommonRestoreOptions{DatabaseReplaceMode: core.DatabaseReplaceModeRelaxed},
		},
		{
			name:        "unknown mode is invalid",
			opts:        CommonRestoreOptions{DatabaseReplaceMode: "foobar"},
			wantErr:     true,
			errContains: "invalid database replace mode",
		},
		{
			name: "known sections are valid",
			opts: CommonRestoreOptions{Section: []string{"pre-data", "data", "post-data"}},
		},
		{
			name:        "unknown section is invalid",
			opts:        CommonRestoreOptions{Section: []string{"bad-section"}},
			wantErr:     true,
			errContains: "unknown section",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.Validate()
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCommonRestoreOptions_RemapDatabase_ViaViper(t *testing.T) {
	tests := []struct {
		name      string
		yaml      string
		wantRemap map[string]string
		wantMode  core.DatabaseReplacementMode
	}{
		{
			name: "remap-database with relaxed mode",
			yaml: `
restore:
  options:
    remap-database:
      mydb: targetdb
    database-replace-mode: relaxed
`,
			wantRemap: map[string]string{"mydb": "targetdb"},
			wantMode:  core.DatabaseReplaceModeRelaxed,
		},
		{
			name: "remap-database without explicit mode",
			yaml: `
restore:
  options:
    remap-database:
      srcdb: dstdb
`,
			wantRemap: map[string]string{"srcdb": "dstdb"},
			wantMode:  "",
		},
		{
			name: "remap-database with multiple entries",
			yaml: `
restore:
  options:
    remap-database:
      db1: tgt1
      db2: tgt2
    database-replace-mode: strict
`,
			wantRemap: map[string]string{"db1": "tgt1", "db2": "tgt2"},
			wantMode:  core.DatabaseReplaceModeStrict,
		},
		{
			name:      "no remap config",
			yaml:      "restore:\n  options:\n    data-only: true\n",
			wantRemap: nil,
			wantMode:  "",
		},
	}

	decoderCfg := func(cfg *mapstructure.DecoderConfig) { cfg.ErrorUnused = true }

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("yaml")
			require.NoError(t, v.ReadConfig(strings.NewReader(tc.yaml)))

			var cfg Config
			require.NoError(t, v.Unmarshal(&cfg, decoderCfg))
			assert.Equal(t, tc.wantRemap, cfg.Restore.Options.RemapDatabase)
			assert.Equal(t, tc.wantMode, cfg.Restore.Options.DatabaseReplaceMode)
			assert.NoError(t, cfg.Restore.Options.Validate())
		})
	}
}
