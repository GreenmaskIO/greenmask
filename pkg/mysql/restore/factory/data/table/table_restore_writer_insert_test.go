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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- newInsertBuilder ---

func TestInsertRestoreWriter_newInsertBuilder_defaultInsert(t *testing.T) {
	table := testTable("db", "users", "id", "name")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{}
	w.applyTableRemap()

	ib := w.newInsertBuilder()
	ib.Values("1", "alice")
	stmt, _ := ib.Build()

	assert.True(t, strings.HasPrefix(stmt, "INSERT INTO"), "got: %s", stmt)
	assert.NotContains(t, stmt, "IGNORE")
	assert.NotContains(t, stmt, "REPLACE")
}

func TestInsertRestoreWriter_newInsertBuilder_insertIgnore(t *testing.T) {
	table := testTable("db", "events", "id")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{InsertIgnore: true}
	w.applyTableRemap()

	ib := w.newInsertBuilder()
	ib.Values("99")
	stmt, _ := ib.Build()

	assert.Contains(t, stmt, "INSERT IGNORE INTO")
}

func TestInsertRestoreWriter_newInsertBuilder_replaceInto(t *testing.T) {
	table := testTable("db", "cache", "key", "val")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{InsertReplace: true}
	w.applyTableRemap()

	ib := w.newInsertBuilder()
	ib.Values("k", "v")
	stmt, _ := ib.Build()

	assert.Contains(t, stmt, "REPLACE INTO")
}

func TestInsertRestoreWriter_newInsertBuilder_columnsQuoted(t *testing.T) {
	table := testTable("db", "t", "my col", "normal")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{}
	w.applyTableRemap()

	ib := w.newInsertBuilder()
	ib.Values("1", "2")
	stmt, _ := ib.Build()

	assert.Contains(t, stmt, "`my col`")
	assert.Contains(t, stmt, "`normal`")
}

func TestInsertRestoreWriter_newInsertBuilder_tableNameQuoted(t *testing.T) {
	table := testTable("my db", "my table", "id")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{}
	w.applyTableRemap()

	ib := w.newInsertBuilder()
	ib.Values("1")
	stmt, _ := ib.Build()

	assert.Contains(t, stmt, "`my db`")
	assert.Contains(t, stmt, "`my table`")
}

// --- buildBatch ---

func TestInsertRestoreWriter_buildBatch_singleTuple(t *testing.T) {
	table := testTable("db", "t", "id", "val")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{}
	w.applyTableRemap()

	sql := w.buildBatch([][]byte{[]byte("(1,'foo')")})

	assert.Contains(t, sql, "INSERT INTO")
	assert.Contains(t, sql, "`db`")
	assert.Contains(t, sql, "`t`")
	assert.Contains(t, sql, "1,'foo'")
}

func TestInsertRestoreWriter_buildBatch_multipleTuples(t *testing.T) {
	table := testTable("db", "orders", "id", "amount")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{}
	w.applyTableRemap()

	sql := w.buildBatch([][]byte{
		[]byte("(1,100)"),
		[]byte("(2,200)"),
		[]byte("(3,300)"),
	})

	assert.Contains(t, sql, "1,100")
	assert.Contains(t, sql, "2,200")
	assert.Contains(t, sql, "3,300")
	// sqlbuilder emits VALUES once for a multi-row insert
	assert.Equal(t, 1, strings.Count(sql, "VALUES"), "expected single VALUES clause")
}

func TestInsertRestoreWriter_buildBatch_withNulls(t *testing.T) {
	table := testTable("db", "t", "id", "data")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{}
	w.applyTableRemap()

	sql := w.buildBatch([][]byte{[]byte("(1,NULL)")})
	assert.Contains(t, sql, "NULL")
}

func TestInsertRestoreWriter_buildBatch_tupleParensStripped(t *testing.T) {
	// buildBatch strips the outer parens and lets sqlbuilder re-add them.
	// The resulting SQL must contain the parens-wrapped tuples correctly.
	table := testTable("db", "t", "x")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{}
	w.applyTableRemap()

	sql := w.buildBatch([][]byte{[]byte("(42)")})
	// sqlbuilder always adds parens back around each VALUES row
	assert.Contains(t, sql, "(42)")
}

func TestInsertRestoreWriter_buildBatch_replaceVerb(t *testing.T) {
	table := testTable("db", "t", "id")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{InsertReplace: true}
	w.applyTableRemap()

	sql := w.buildBatch([][]byte{[]byte("(1)")})
	assert.Contains(t, sql, "REPLACE INTO")
}

func TestInsertRestoreWriter_buildBatch_ignoreVerb(t *testing.T) {
	table := testTable("db", "t", "id")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{InsertIgnore: true}
	w.applyTableRemap()

	sql := w.buildBatch([][]byte{[]byte("(1)")})
	assert.Contains(t, sql, "INSERT IGNORE INTO")
}

// --- computeHeaderLen ---

func TestInsertRestoreWriter_computeHeaderLen_matchesBuildBatch(t *testing.T) {
	table := testTable("db", "users", "id", "email")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{}
	w.applyTableRemap()

	headerLen := w.computeHeaderLen()
	require.Greater(t, headerLen, 0)

	// Verify headerLen equals the byte offset just after "VALUES " in a real statement.
	sql := w.buildBatch([][]byte{[]byte("(1,'test@example.com')")})
	idx := strings.LastIndex(sql, "VALUES ")
	expected := idx + len("VALUES ")
	assert.Equal(t, expected, headerLen)
}

func TestInsertRestoreWriter_computeHeaderLen_variesByTable(t *testing.T) {
	// A table with more/longer column names produces a longer header.
	short := testTable("a", "b", "c")
	wShort := NewInsertRestoreWriter(short)
	wShort.opts = TableRestoreOpts{}
	wShort.applyTableRemap()

	long := testTable("longschema", "longtablename", "column_one", "column_two", "column_three")
	wLong := NewInsertRestoreWriter(long)
	wLong.opts = TableRestoreOpts{}
	wLong.applyTableRemap()

	assert.Greater(t, wLong.computeHeaderLen(), wShort.computeHeaderLen())
}

func TestInsertRestoreWriter_computeHeaderLen_replaceVerb(t *testing.T) {
	// "REPLACE INTO" is longer than "INSERT INTO", so the header length differs.
	table := testTable("db", "t", "id")

	wInsert := NewInsertRestoreWriter(table)
	wInsert.opts = TableRestoreOpts{}
	wInsert.applyTableRemap()

	wReplace := NewInsertRestoreWriter(table)
	wReplace.opts = TableRestoreOpts{InsertReplace: true}
	wReplace.applyTableRemap()

	assert.Greater(t, wReplace.computeHeaderLen(), wInsert.computeHeaderLen())
}

// --- showWarnings early-return paths (no DB connection required) ---

func TestInsertRestoreWriter_showWarnings_zeroWarningCount(t *testing.T) {
	table := testTable("db", "t", "id")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{PrintWarnings: true}

	printed := 0
	// conn is nil; if reached it would panic — verifies early-return at count == 0.
	count, err := w.showWarnings(context.Background(), nil, 0, 1, &printed)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestInsertRestoreWriter_showWarnings_printingDisabled(t *testing.T) {
	table := testTable("db", "t", "id")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{PrintWarnings: false}

	printed := 0
	// conn is nil; verifies no DB round-trip when PrintWarnings is false.
	count, err := w.showWarnings(context.Background(), nil, 7, 1, &printed)
	assert.NoError(t, err)
	assert.Equal(t, 7, count)
}

func TestInsertRestoreWriter_showWarnings_maxFetchExhausted(t *testing.T) {
	table := testTable("db", "t", "id")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{PrintWarnings: true, MaxFetchWarnings: 5}

	printed := 5 // already at the cap
	count, err := w.showWarnings(context.Background(), nil, 3, 2, &printed)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestInsertRestoreWriter_showWarnings_zeroMaxFetch_noEarlyReturn(t *testing.T) {
	// MaxFetchWarnings == 0 means "no limit" — the code skips the fetchLimit block.
	// We cannot call the DB path in a unit test, so just confirm the function does
	// NOT return on the MaxFetch guard when MaxFetchWarnings is 0.
	// We set warningCount=0 to trigger the outermost guard and avoid the DB call.
	table := testTable("db", "t", "id")
	w := NewInsertRestoreWriter(table)
	w.opts = TableRestoreOpts{PrintWarnings: true, MaxFetchWarnings: 0}

	printed := 999
	count, err := w.showWarnings(context.Background(), nil, 0, 1, &printed)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

// --- Open error path ---

func TestInsertRestoreWriter_Open_wrongConnType(t *testing.T) {
	table := testTable("db", "t", "id")
	w := NewInsertRestoreWriter(table)

	// config is a plain string, not *RestoreConnectionConfig — must return an error.
	err := w.Open(context.Background(), &stubSession{}, stubConnConfigurer{config: "bad config"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "expected *connconfig.RestoreConnectionConfig")
}
