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

package delete

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/heartbeat"
	"github.com/greenmaskio/greenmask/pkg/common/mocks"
)

// ---- helpers ----------------------------------------------------------------

func hbReader(status heartbeat.Status) io.ReadCloser {
	b, _ := json.Marshal(heartbeat.Heartbeat{Status: status, UpdatedAt: time.Now()})
	return io.NopCloser(bytes.NewReader(b))
}

func metaReader(startedAt time.Time, databases ...string) io.ReadCloser {
	md := core.Metadata{StartedAt: startedAt, Databases: databases}
	b, _ := json.Marshal(md)
	return io.NopCloser(bytes.NewReader(b))
}

// dumpMock creates a per-dump storage mock. When withMeta is true, the mock
// also returns metadata.json (only called for heartbeat.StatusDone dumps).
func dumpMock(id string, hbStatus heartbeat.Status, startedAt time.Time, withMeta bool) *mocks.StorageMock {
	m := mocks.NewStorageMock()
	m.On("Dirname").Return(id)
	m.On("GetObject", mock.Anything, heartbeat.FileName).Return(hbReader(hbStatus), nil)
	if withMeta {
		m.On("GetObject", mock.Anything, metadataFileName).Return(metaReader(startedAt, "testdb"), nil)
	}
	return m
}

// rootMock creates a root storage mock whose ListDir returns the given dumps.
func rootMock(dumps ...*mocks.StorageMock) *mocks.StorageMock {
	dirs := make([]core.Storager, len(dumps))
	for i, d := range dumps {
		dirs[i] = d
	}
	root := mocks.NewStorageMock()
	root.On("ListDir", mock.Anything).Return([]string{}, dirs, nil)
	return root
}

func newDeleter(root *mocks.StorageMock) *Deleter {
	return New(root, 15*time.Minute)
}

// ---- ByDumpID ---------------------------------------------------------------

func TestByDumpID(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name    string
		dumpID  string
		dryRun  bool
		wantDel bool
		wantErr bool
	}{
		{
			name:    "deletes existing dump",
			dumpID:  "dump-001",
			wantDel: true,
		},
		{
			name:   "dry-run does not call DeleteAll",
			dumpID: "dump-001",
			dryRun: true,
		},
		{
			name:    "returns error when dump not found",
			dumpID:  "dump-999",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d1 := dumpMock("dump-001", heartbeat.StatusDone, now, true)
			root := rootMock(d1)
			if tc.wantDel {
				root.On("DeleteAll", mock.Anything, tc.dumpID).Return(nil)
			}

			err := newDeleter(root).ByDumpID(ctx, tc.dumpID, tc.dryRun)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.wantDel {
				root.AssertCalled(t, "DeleteAll", mock.Anything, tc.dumpID)
			} else {
				root.AssertNotCalled(t, "DeleteAll")
			}
		})
	}
}

// ---- PruneFailed ------------------------------------------------------------

func TestPruneFailed(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name        string
		pruneUnsafe bool
		dryRun      bool
		wantDeleted []string
		wantKept    []string
	}{
		{
			name:        "deletes failed dumps, skips done and unknown-or-failed",
			wantDeleted: []string{"dump-002"},
			wantKept:    []string{"dump-001", "dump-003"},
		},
		{
			name:        "prune-unsafe also deletes unknown-or-failed",
			pruneUnsafe: true,
			wantDeleted: []string{"dump-002", "dump-003"},
			wantKept:    []string{"dump-001"},
		},
		{
			name:   "dry-run does not call DeleteAll",
			dryRun: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d1 := dumpMock("dump-001", heartbeat.StatusDone, now, true)
			d2 := dumpMock("dump-002", heartbeat.StatusFailed, time.Time{}, false)
			// dump-003: heartbeat read error → readDumpInfo returns error → unknown-or-failed
			d3 := mocks.NewStorageMock()
			d3.On("Dirname").Return("dump-003")
			d3.On("GetObject", mock.Anything, heartbeat.FileName).Return(nil, fmt.Errorf("heartbeat missing"))

			root := rootMock(d1, d2, d3)
			if !tc.dryRun {
				for _, id := range tc.wantDeleted {
					root.On("DeleteAll", mock.Anything, id).Return(nil)
				}
			}

			err := newDeleter(root).PruneFailed(ctx, tc.pruneUnsafe, tc.dryRun)
			require.NoError(t, err)

			if tc.dryRun {
				root.AssertNotCalled(t, "DeleteAll")
				return
			}
			for _, id := range tc.wantDeleted {
				root.AssertCalled(t, "DeleteAll", mock.Anything, id)
			}
			for _, id := range tc.wantKept {
				root.AssertNotCalled(t, "DeleteAll", mock.Anything, id)
			}
		})
	}
}

// ---- BeforeDate -------------------------------------------------------------

func TestBeforeDate(t *testing.T) {
	ctx := context.Background()

	old := time.Now().Add(-48 * time.Hour)
	recent := time.Now().Add(-1 * time.Hour)
	cutoff := time.Now().Add(-24 * time.Hour).Format(time.RFC3339)

	tests := []struct {
		name        string
		dateStr     string
		dryRun      bool
		wantDeleted []string
		wantKept    []string
		wantErr     bool
	}{
		{
			name:        "deletes dumps before cutoff",
			dateStr:     cutoff,
			wantDeleted: []string{"dump-001"},
			wantKept:    []string{"dump-002"},
		},
		{
			name:    "dry-run does not call DeleteAll",
			dateStr: cutoff,
			dryRun:  true,
		},
		{
			name:    "invalid date returns error",
			dateStr: "not-a-date",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d1 := dumpMock("dump-001", heartbeat.StatusDone, old, true)
			d2 := dumpMock("dump-002", heartbeat.StatusDone, recent, true)
			root := rootMock(d1, d2)

			if !tc.dryRun {
				for _, id := range tc.wantDeleted {
					root.On("DeleteAll", mock.Anything, id).Return(nil)
				}
			}

			err := newDeleter(root).BeforeDate(ctx, tc.dateStr, tc.dryRun)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tc.dryRun {
				root.AssertNotCalled(t, "DeleteAll")
				return
			}
			for _, id := range tc.wantDeleted {
				root.AssertCalled(t, "DeleteAll", mock.Anything, id)
			}
			for _, id := range tc.wantKept {
				root.AssertNotCalled(t, "DeleteAll", mock.Anything, id)
			}
		})
	}
}

// ---- RetainFor --------------------------------------------------------------

func TestRetainFor(t *testing.T) {
	ctx := context.Background()

	old := time.Now().Add(-72 * time.Hour)
	recent := time.Now().Add(-1 * time.Hour)

	tests := []struct {
		name        string
		retainFor   string
		dryRun      bool
		wantDeleted []string
		wantKept    []string
		wantErr     bool
	}{
		{
			name:        "deletes dumps older than duration",
			retainFor:   "48h",
			wantDeleted: []string{"dump-001"},
			wantKept:    []string{"dump-002"},
		},
		{
			name:      "dry-run does not call DeleteAll",
			retainFor: "48h",
			dryRun:    true,
		},
		{
			name:      "invalid duration returns error",
			retainFor: "not-a-duration",
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d1 := dumpMock("dump-001", heartbeat.StatusDone, old, true)
			d2 := dumpMock("dump-002", heartbeat.StatusDone, recent, true)
			root := rootMock(d1, d2)

			if !tc.dryRun {
				for _, id := range tc.wantDeleted {
					root.On("DeleteAll", mock.Anything, id).Return(nil)
				}
			}

			err := newDeleter(root).RetainFor(ctx, tc.retainFor, tc.dryRun)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tc.dryRun {
				root.AssertNotCalled(t, "DeleteAll")
				return
			}
			for _, id := range tc.wantDeleted {
				root.AssertCalled(t, "DeleteAll", mock.Anything, id)
			}
			for _, id := range tc.wantKept {
				root.AssertNotCalled(t, "DeleteAll", mock.Anything, id)
			}
		})
	}
}

// ---- RetainRecent -----------------------------------------------------------

func TestRetainRecent(t *testing.T) {
	ctx := context.Background()

	// dump IDs sort newest-first: dump-003 > dump-002 > dump-001
	now := time.Now()

	tests := []struct {
		name        string
		n           int
		dryRun      bool
		wantDeleted []string
		wantKept    []string
	}{
		{
			name:        "keeps 2 most recent, deletes oldest",
			n:           2,
			wantDeleted: []string{"dump-001"},
			wantKept:    []string{"dump-003", "dump-002"},
		},
		{
			name:        "keeps all when n >= count",
			n:           10,
			wantDeleted: []string{},
			wantKept:    []string{"dump-003", "dump-002", "dump-001"},
		},
		{
			name:        "keeps none when n=0",
			n:           0,
			wantDeleted: []string{"dump-003", "dump-002", "dump-001"},
		},
		{
			name:   "dry-run does not call DeleteAll",
			n:      1,
			dryRun: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d1 := dumpMock("dump-001", heartbeat.StatusDone, now.Add(-3*time.Hour), true)
			d2 := dumpMock("dump-002", heartbeat.StatusDone, now.Add(-2*time.Hour), true)
			d3 := dumpMock("dump-003", heartbeat.StatusDone, now.Add(-1*time.Hour), true)
			root := rootMock(d1, d2, d3)

			if !tc.dryRun {
				for _, id := range tc.wantDeleted {
					root.On("DeleteAll", mock.Anything, id).Return(nil)
				}
			}

			err := newDeleter(root).RetainRecent(ctx, tc.n, tc.dryRun)
			require.NoError(t, err)

			if tc.dryRun {
				root.AssertNotCalled(t, "DeleteAll")
				return
			}
			for _, id := range tc.wantDeleted {
				root.AssertCalled(t, "DeleteAll", mock.Anything, id)
			}
			for _, id := range tc.wantKept {
				root.AssertNotCalled(t, "DeleteAll", mock.Anything, id)
			}
		})
	}
}

// ---- toStatus ---------------------------------------------------------------

func TestToStatus(t *testing.T) {
	tests := []struct {
		hb   heartbeat.Status
		want DumpStatus
	}{
		{heartbeat.StatusDone, DumpStatusDone},
		{heartbeat.StatusInProgress, DumpStatusInProgress},
		{heartbeat.StatusFailed, DumpStatusFailed},
		{heartbeat.Status("unknown"), DumpStatusUnknownOrFailed},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, toStatus(tc.hb))
	}
}
