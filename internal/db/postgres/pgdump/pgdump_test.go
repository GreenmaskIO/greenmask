// Copyright 2023 Greenmask
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

package pgdump

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptions_GetParams_excludeExtension(t *testing.T) {
	options := &Options{
		Jobs:            -1,
		Compression:     -1,
		LockWaitTimeout: -1,
		Port:            pgDefaultPort,
	}

	assert.NotContains(t, options.GetParams(), "--exclude-extension")

	options.ExcludeExtension = []string{"pg_stat_statements", "pgcrypto"}
	assert.Equal(
		t,
		[]string{
			"--exclude-extension", "pg_stat_statements",
			"--exclude-extension", "pgcrypto",
		},
		options.GetParams(),
	)
}

func TestOptions_GetParams_lockWaitTimeout(t *testing.T) {
	options := &Options{
		Jobs:            -1,
		Compression:     -1,
		LockWaitTimeout: -1,
		Port:            pgDefaultPort,
	}

	assert.NotContains(t, options.GetParams(), "--lock-wait-timeout")

	options.LockWaitTimeout = 5000
	assert.Equal(t, []string{"--lock-wait-timeout", "5000"}, options.GetParams())
}

func TestOptions_Validate(t *testing.T) {
	tests := []struct {
		name    string
		opts    Options
		wantErr string
	}{
		{name: "empty", opts: Options{}},
		{name: "data-only", opts: Options{DataOnly: true}},
		{name: "data-only with create", opts: Options{DataOnly: true, Create: true}},
		{name: "schema-only with clean", opts: Options{SchemaOnly: true, Clean: true}},
		{
			name:    "data-only with schema-only",
			opts:    Options{DataOnly: true, SchemaOnly: true},
			wantErr: "options --data-only and --schema-only cannot be used together",
		},
		{
			name:    "data-only with clean",
			opts:    Options{DataOnly: true, Clean: true},
			wantErr: "options --data-only and --clean cannot be used together",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestOptions_HeaderOnlyOptions(t *testing.T) {
	options := &Options{
		Jobs:               4,
		Compression:        -1,
		LockWaitTimeout:    3000,
		Port:               pgDefaultPort,
		DataOnly:           true,
		Blobs:              true,
		Create:             true,
		ExcludeSchema:      []string{"tmp"},
		Table:              []string{"app.t1"},
		ExcludeTableData:   []string{"app.t3"},
		IncludeForeignData: []string{"srv"},
		Section:            "data",
		Snapshot:           "00000003-00000002-1",
		DbName:             "postgres",
	}

	res := options.HeaderOnlyOptions()

	assert.Equal(t,
		[]string{
			"--lock-wait-timeout", "3000",
			"--data-only",
			"--no-blobs",
			"--create",
			"--exclude-schema", "*",
			"--exclude-table-data", "app.t3",
			"--snapshot", "00000003-00000002-1",
			"--dbname", "postgres",
		},
		res.GetParams(),
	)
	assert.True(t, options.DataOnly, "the source options must not change")
	assert.Equal(t, []string{"tmp"}, options.ExcludeSchema, "the source options must not change")
}
