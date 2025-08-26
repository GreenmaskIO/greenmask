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

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestRestore_sortTocEntriesInTopoOrder_BugFix_IncludesSequences(t *testing.T) {
	// Setup: Tables and sequences mixed together
	metadata := &storage.Metadata{
		DumpIdsOrder: []int32{100, 101}, // Only tables are in topological order
		DumpIdsToTableOid: map[int32]toolkit.Oid{
			100: 1000,
			101: 1001,
		},
		DatabaseSchema: toolkit.DatabaseSchema{
			{Oid: 1000, Schema: "public", Name: "users"},
			{Oid: 1001, Schema: "public", Name: "posts"},
		},
		Cycles: [][]string{},
	}

	entries := []*toc.Entry{
		{
			DumpId: 100,
			Desc:   strPtr(toc.TableDataDesc),
			Tag:    strPtr("users"),
		},
		{
			DumpId: 101,
			Desc:   strPtr(toc.TableDataDesc),
			Tag:    strPtr("posts"),
		},
		{
			DumpId: 200,
			Desc:   strPtr(toc.SequenceSetDesc),
			Tag:    strPtr("users_id_seq"),
		},
		{
			DumpId: 201,
			Desc:   strPtr(toc.SequenceSetDesc),
			Tag:    strPtr("posts_id_seq"),
		},
	}

	r := &Restore{
		metadata: metadata,
	}

	result := r.sortTocEntriesInTopoOrder(entries)

	require.Len(t, result, 4, "Should include both tables and sequences")

	// Verify: Order should be tables first (topologically sorted), then sequences
	assert.Equal(t, int32(100), result[0].DumpId, "First should be users table")
	assert.Equal(t, int32(101), result[1].DumpId, "Second should be posts table")
	assert.Equal(t, int32(200), result[2].DumpId, "Third should be users_id_seq")
	assert.Equal(t, int32(201), result[3].DumpId, "Fourth should be posts_id_seq")

	// Verify: Entry types are correct
	assert.Equal(t, toc.TableDataDesc, *result[0].Desc)
	assert.Equal(t, toc.TableDataDesc, *result[1].Desc)
	assert.Equal(t, toc.SequenceSetDesc, *result[2].Desc)
	assert.Equal(t, toc.SequenceSetDesc, *result[3].Desc)

	// Verify: No duplicates
	seen := make(map[int32]bool)
	for _, entry := range result {
		assert.False(t, seen[entry.DumpId], "No duplicate DumpIds should exist")
		seen[entry.DumpId] = true
	}
}

func TestRestore_sortTocEntriesInTopoOrder_PreserveOriginalBehavior(t *testing.T) {
	// Setup: Only tables, no sequences
	metadata := &storage.Metadata{
		DumpIdsOrder: []int32{100, 101},
		DumpIdsToTableOid: map[int32]toolkit.Oid{
			100: 1000,
			101: 1001,
		},
		DatabaseSchema: toolkit.DatabaseSchema{
			{Oid: 1000, Schema: "public", Name: "users"},
			{Oid: 1001, Schema: "public", Name: "posts"},
		},
		Cycles: [][]string{},
	}

	entries := []*toc.Entry{
		{
			DumpId: 100,
			Desc:   strPtr(toc.TableDataDesc),
			Tag:    strPtr("users"),
		},
		{
			DumpId: 101,
			Desc:   strPtr(toc.TableDataDesc),
			Tag:    strPtr("posts"),
		},
	}

	r := &Restore{
		metadata: metadata,
	}

	// Execute
	result := r.sortTocEntriesInTopoOrder(entries)

	// Verify: Only tables should be returned
	require.Len(t, result, 2, "Should only include tables")

	// Verify: Order preserved (topological)
	assert.Equal(t, int32(100), result[0].DumpId, "First should be users table")
	assert.Equal(t, int32(101), result[1].DumpId, "Second should be posts table")

	// Verify: All are tables
	assert.Equal(t, toc.TableDataDesc, *result[0].Desc)
	assert.Equal(t, toc.TableDataDesc, *result[1].Desc)
}

// Helper functions
func strPtr(s string) *string {
	return &s
}
