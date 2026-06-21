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

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

func TestFactory_Kind(t *testing.T) {
	f := NewFactory()
	assert.Equal(t, core.ObjectKindMysqlTable, f.Kind())
}

func TestFactory_New(t *testing.T) {
	validTable := testTable("mydb", "users", "id", "name")
	ordersTable := testTable("mydb", "orders", "id", "total")

	tests := []struct {
		name          string
		spec          core.ObjectRestoreSpec
		wantRestorer  bool   // true → expect *TableRestorer
		wantErrSubstr string // non-empty → expect error containing this substring
	}{
		{
			name: "insert format with valid table payload",
			spec: core.ObjectRestoreSpec{
				Payload:  validTable,
				Format:   core.DumpFormatInsert,
				Filename: "users.data",
			},
			wantRestorer: true,
		},
		{
			name: "invalid payload type",
			spec: core.ObjectRestoreSpec{
				Payload: "not a table",
				Format:  core.DumpFormatInsert,
			},
			wantErrSubstr: "expected *core.Table payload",
		},
		{
			name: "nil payload",
			spec: core.ObjectRestoreSpec{
				Payload: nil,
				Format:  core.DumpFormatInsert,
			},
			wantErrSubstr: "expected *core.Table payload",
		},
		{
			name: "csv format is unsupported",
			spec: core.ObjectRestoreSpec{
				Payload: ordersTable,
				Format:  core.DumpFormatCsv,
			},
			wantErrSubstr: "unsupported dump format",
		},
		{
			name: "unknown format",
			spec: core.ObjectRestoreSpec{
				Payload: validTable,
				Format:  core.DumpFormat("parquet"),
			},
			wantErrSubstr: "unsupported dump format",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := NewFactory()
			obj, err := f.New(tc.spec)

			if tc.wantErrSubstr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrSubstr)
				assert.Nil(t, obj)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, obj)
			if tc.wantRestorer {
				_, ok := obj.(*TableRestorer)
				assert.True(t, ok, "expected *TableRestorer, got %T", obj)
			}
		})
	}
}
