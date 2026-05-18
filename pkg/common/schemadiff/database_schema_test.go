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

package schemadiff

import (
	"testing"

	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// col is a helper to build a config.Column concisely.
func col(idx int, name string, typeOID models.VirtualOID, typeName string) models.Column {
	return models.Column{Idx: idx, Name: name, TypeOID: typeOID, TypeName: typeName}
}

// mkTable is a helper to build a config.Table concisely.
func mkTable(id int, schema, name string, cols ...models.Column) models.Table {
	return models.Table{ID: id, Schema: schema, Name: name, Columns: cols}
}

func TestDiff(t *testing.T) {
	colID := col(0, "id", 23, "integer")

	tests := []struct {
		name     string
		previous DatabaseSchema
		current  DatabaseSchema
		wantLen  int
		wantFunc func(t *testing.T, nodes []models.DiffNode)
	}{
		{
			name:     "new table not in previous",
			previous: DatabaseSchema{},
			current:  DatabaseSchema{mkTable(1, "public", "users", colID)},
			wantLen:  1,
			wantFunc: func(t *testing.T, nodes []models.DiffNode) {
				assert.Equal(t, models.TableCreatedDiffEvent, nodes[0].Event)
				assert.Equal(t, models.DiffEventMsgs[models.TableCreatedDiffEvent], nodes[0].Msg)
				assert.Equal(t, "public", nodes[0].Signature["SchemaName"])
				assert.Equal(t, "users", nodes[0].Signature["TableName"])
				assert.Equal(t, "1", nodes[0].Signature["TableID"])
			},
		},
		{
			name:     "table found by ID with no changes",
			previous: DatabaseSchema{mkTable(1, "public", "users", colID)},
			current:  DatabaseSchema{mkTable(1, "public", "users", colID)},
			wantLen:  0,
		},
		{
			name:     "table ID changed but found by name (re-creation, no structural diff)",
			previous: DatabaseSchema{mkTable(1, "public", "users", colID)},
			current:  DatabaseSchema{mkTable(99, "public", "users", colID)},
			wantLen:  0,
		},
		{
			name:     "empty current produces no events",
			previous: DatabaseSchema{mkTable(1, "public", "users")},
			current:  DatabaseSchema{},
			wantLen:  0,
		},
		{
			name: "multiple tables, one new",
			previous: DatabaseSchema{
				mkTable(1, "public", "users", colID),
			},
			current: DatabaseSchema{
				mkTable(1, "public", "users", colID),
				mkTable(2, "public", "orders", colID),
			},
			wantLen: 1,
			wantFunc: func(t *testing.T, nodes []models.DiffNode) {
				assert.Equal(t, models.TableCreatedDiffEvent, nodes[0].Event)
				assert.Equal(t, "orders", nodes[0].Signature["TableName"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodes := tc.previous.Diff(tc.current)
			require.Len(t, nodes, tc.wantLen)
			if tc.wantFunc != nil {
				tc.wantFunc(t, nodes)
			}
		})
	}
}

func TestDiffTables(t *testing.T) {
	colID := col(0, "id", 23, "integer")

	tests := []struct {
		name     string
		previous models.Table
		current  models.Table
		wantLen  int
		wantFunc func(t *testing.T, nodes []models.DiffNode)
	}{
		{
			name:     "no changes",
			previous: mkTable(1, "public", "users", colID),
			current:  mkTable(1, "public", "users", colID),
			wantLen:  0,
		},
		{
			name:     "schema moved",
			previous: mkTable(1, "public", "users"),
			current:  mkTable(1, "private", "users"),
			wantLen:  1,
			wantFunc: func(t *testing.T, nodes []models.DiffNode) {
				assert.Equal(t, models.TableMovedToAnotherSchemaDiffEvent, nodes[0].Event)
				assert.Equal(t, models.DiffEventMsgs[models.TableMovedToAnotherSchemaDiffEvent], nodes[0].Msg)
				assert.Equal(t, "public", nodes[0].Signature["PreviousSchemaName"])
				assert.Equal(t, "private", nodes[0].Signature["CurrentSchemaName"])
				assert.Equal(t, "users", nodes[0].Signature["TableName"])
				assert.Equal(t, "1", nodes[0].Signature["TableID"])
			},
		},
		{
			name:     "renamed",
			previous: mkTable(1, "public", "users"),
			current:  mkTable(1, "public", "accounts"),
			wantLen:  1,
			wantFunc: func(t *testing.T, nodes []models.DiffNode) {
				assert.Equal(t, models.TableRenamedDiffEvent, nodes[0].Event)
				assert.Equal(t, models.DiffEventMsgs[models.TableRenamedDiffEvent], nodes[0].Msg)
				assert.Equal(t, "users", nodes[0].Signature["PreviousTableName"])
				assert.Equal(t, "accounts", nodes[0].Signature["CurrentTableName"])
				assert.Equal(t, "public", nodes[0].Signature["SchemaName"])
				assert.Equal(t, "1", nodes[0].Signature["TableID"])
			},
		},
		{
			name:     "schema moved and renamed",
			previous: mkTable(1, "public", "users"),
			current:  mkTable(1, "private", "accounts"),
			wantLen:  2,
			wantFunc: func(t *testing.T, nodes []models.DiffNode) {
				events := []string{nodes[0].Event, nodes[1].Event}
				assert.Contains(t, events, models.TableMovedToAnotherSchemaDiffEvent)
				assert.Contains(t, events, models.TableRenamedDiffEvent)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodes := diffTables(tc.previous, tc.current)
			require.Len(t, nodes, tc.wantLen)
			if tc.wantFunc != nil {
				tc.wantFunc(t, nodes)
			}
		})
	}
}

func TestDiffTableColumns(t *testing.T) {
	tests := []struct {
		name     string
		previous models.Table
		current  models.Table
		wantLen  int
		wantFunc func(t *testing.T, nodes []models.DiffNode)
	}{
		{
			name:     "no changes",
			previous: mkTable(1, "public", "users", col(0, "id", 23, "integer")),
			current:  mkTable(1, "public", "users", col(0, "id", 23, "integer")),
			wantLen:  0,
		},
		{
			name:     "new column",
			previous: mkTable(1, "public", "users", col(0, "id", 23, "integer")),
			current:  mkTable(1, "public", "users", col(0, "id", 23, "integer"), col(1, "email", 25, "text")),
			wantLen:  1,
			wantFunc: func(t *testing.T, nodes []models.DiffNode) {
				assert.Equal(t, models.ColumnCreatedDiffEvent, nodes[0].Event)
				assert.Equal(t, models.DiffEventMsgs[models.ColumnCreatedDiffEvent], nodes[0].Msg)
				assert.Equal(t, "email", nodes[0].Signature["ColumnName"])
				assert.Equal(t, "text", nodes[0].Signature["ColumnType"])
				assert.Equal(t, "public", nodes[0].Signature["TableSchema"])
				assert.Equal(t, "users", nodes[0].Signature["TableName"])
			},
		},
		{
			name:     "column renamed",
			previous: mkTable(1, "public", "users", col(0, "email", 25, "text")),
			current:  mkTable(1, "public", "users", col(0, "email_address", 25, "text")),
			wantLen:  1,
			wantFunc: func(t *testing.T, nodes []models.DiffNode) {
				assert.Equal(t, models.ColumnRenamedDiffEvent, nodes[0].Event)
				assert.Equal(t, models.DiffEventMsgs[models.ColumnRenamedDiffEvent], nodes[0].Msg)
				assert.Equal(t, "email", nodes[0].Signature["PreviousColumnName"])
				assert.Equal(t, "email_address", nodes[0].Signature["CurrentColumnName"])
			},
		},
		{
			name:     "column type changed",
			previous: mkTable(1, "public", "users", col(0, "score", 23, "integer")),
			current:  mkTable(1, "public", "users", col(0, "score", 700, "float8")),
			wantLen:  1,
			wantFunc: func(t *testing.T, nodes []models.DiffNode) {
				assert.Equal(t, models.ColumnTypeChangedDiffEvent, nodes[0].Event)
				assert.Equal(t, models.DiffEventMsgs[models.ColumnTypeChangedDiffEvent], nodes[0].Msg)
				assert.Equal(t, "score", nodes[0].Signature["ColumnName"])
				assert.Equal(t, "integer", nodes[0].Signature["PreviousColumnType"])
				assert.Equal(t, "23", nodes[0].Signature["PreviousColumnTypeOID"])
				assert.Equal(t, "float8", nodes[0].Signature["CurrentColumnType"])
				assert.Equal(t, "700", nodes[0].Signature["CurrentColumnTypeOID"])
			},
		},
		{
			name:     "column renamed and type changed",
			previous: mkTable(1, "public", "users", col(0, "score", 23, "integer")),
			current:  mkTable(1, "public", "users", col(0, "rating", 700, "float8")),
			wantLen:  2,
			wantFunc: func(t *testing.T, nodes []models.DiffNode) {
				events := []string{nodes[0].Event, nodes[1].Event}
				assert.Contains(t, events, models.ColumnRenamedDiffEvent)
				assert.Contains(t, events, models.ColumnTypeChangedDiffEvent)
			},
		},
		{
			name:     "column found by name fallback (Idx changed, no other diff)",
			previous: mkTable(1, "public", "users", col(2, "email", 25, "text")),
			current:  mkTable(1, "public", "users", col(5, "email", 25, "text")),
			wantLen:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodes := diffTableColumns(tc.previous, tc.current)
			require.Len(t, nodes, tc.wantLen)
			if tc.wantFunc != nil {
				tc.wantFunc(t, nodes)
			}
		})
	}
}

func TestFindColumnByIdx(t *testing.T) {
	t2 := mkTable(1, "public", "users",
		col(0, "id", 23, "integer"),
		col(1, "name", 25, "text"),
	)

	tests := []struct {
		name    string
		idx     int
		wantOk  bool
		wantCol string
	}{
		{"found", 1, true, "name"},
		{"not found", 99, false, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, ok := findColumnByIdx(t2, tc.idx)
			assert.Equal(t, tc.wantOk, ok)
			if tc.wantOk {
				assert.Equal(t, tc.wantCol, c.Name)
			}
		})
	}
}

func TestFindColumnByName(t *testing.T) {
	t2 := mkTable(1, "public", "users",
		col(0, "id", 23, "integer"),
		col(1, "email", 25, "text"),
	)

	tests := []struct {
		name    string
		colName string
		wantOk  bool
		wantIdx int
	}{
		{"found", "email", true, 1},
		{"not found", "missing", false, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, ok := findColumnByName(t2, tc.colName)
			assert.Equal(t, tc.wantOk, ok)
			if tc.wantOk {
				assert.Equal(t, tc.wantIdx, c.Idx)
			}
		})
	}
}
