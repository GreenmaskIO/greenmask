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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

func testTable(schema, name string, cols ...string) *core.Table {
	columns := make([]core.Column, len(cols))
	for i, c := range cols {
		columns[i] = core.Column{Name: c}
	}
	return &core.Table{Schema: schema, Name: name, Columns: columns}
}

func TestInsertRestoreWriter_applyTableRemap(t *testing.T) {
	tests := []struct {
		name           string
		originalSchema string
		remap          map[string]string
		expectedSchema string
	}{
		{
			name:           "mapped schema is replaced",
			originalSchema: "src",
			remap:          map[string]string{"src": "dst"},
			expectedSchema: "dst",
		},
		{
			name:           "unmapped schema is unchanged",
			originalSchema: "other",
			remap:          map[string]string{"src": "dst"},
			expectedSchema: "other",
		},
		{
			name:           "nil map leaves schema unchanged",
			originalSchema: "mydb",
			remap:          nil,
			expectedSchema: "mydb",
		},
		{
			name:           "empty map leaves schema unchanged",
			originalSchema: "mydb",
			remap:          map[string]string{},
			expectedSchema: "mydb",
		},
		{
			name:           "multiple entries — correct key selected",
			originalSchema: "b",
			remap:          map[string]string{"a": "x", "b": "y"},
			expectedSchema: "y",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			orig := testTable(tc.originalSchema, "users", "id")
			w := NewInsertRestoreWriter(orig)
			w.opts = TableRestoreOpts{RemapDatabase: tc.remap}

			w.applyTableRemap()

			assert.Equal(t, tc.expectedSchema, w.table.Schema)
		})
	}
}

// TestInsertRestoreWriter_applyTableRemap_doesNotMutateOriginal confirms that
// applyTableRemap makes a copy — the original *core.Table is not modified.
func TestInsertRestoreWriter_applyTableRemap_doesNotMutateOriginal(t *testing.T) {
	orig := testTable("src", "orders", "id", "amount")
	w := NewInsertRestoreWriter(orig)
	w.opts = TableRestoreOpts{RemapDatabase: map[string]string{"src": "dst"}}

	w.applyTableRemap()

	assert.Equal(t, "src", orig.Schema, "original table must not be mutated")
	assert.Equal(t, "dst", w.table.Schema, "writer must use remapped schema")
}

// TestInsertRestoreWriter_buildBatch_usesRemappedSchema verifies that the SQL
// produced by buildBatch references the remapped database name after applyTableRemap.
func TestInsertRestoreWriter_buildBatch_usesRemappedSchema(t *testing.T) {
	table := testTable("src", "users", "id", "name")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{RemapDatabase: map[string]string{"src": "dst"}}
	w.applyTableRemap()

	sql := w.buildBatch([][]byte{[]byte("(1,'alice')")})

	assert.Contains(t, sql, "`dst`", "remapped schema must appear in SQL")
	assert.NotContains(t, sql, "`src`", "original schema must not appear in SQL")
}

// TestInsertRestoreWriter_buildBatch_noRemap confirms that without a mapping
// the original schema name is used in the SQL.
func TestInsertRestoreWriter_buildBatch_noRemap(t *testing.T) {
	table := testTable("production", "orders", "id")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{} // no remap
	w.applyTableRemap()

	sql := w.buildBatch([][]byte{[]byte("(42)")})

	assert.Contains(t, sql, "`production`")
}
