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

package heartbeat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHeartbeat_GetStatus(t *testing.T) {
	type test struct {
		name     string
		since    time.Duration // Duration to consider for the heartbeat status
		original Heartbeat
		result   Status
	}

	tests := []test{
		{
			name:  "status terminateWithStatus",
			since: 1 * time.Minute,
			original: Heartbeat{
				Status:    StatusDone,
				UpdatedAt: time.Now(),
			},
			result: StatusDone,
		},
		{
			name:  "status failed",
			since: 1 * time.Minute,
			original: Heartbeat{
				Status:    StatusFailed,
				UpdatedAt: time.Now(),
			},
			result: StatusFailed,
		},
		{
			name:  "status in progress, updated recently",
			since: 3 * time.Minute,
			original: Heartbeat{
				Status:    StatusInProgress,
				UpdatedAt: time.Now().Add(-1 * time.Minute), // Updated 1 minute ago
			},
			result: StatusInProgress,
		},
		{
			name:  "status in progress, updated too long ago",
			since: 1 * time.Minute,
			original: Heartbeat{
				Status:    StatusInProgress,
				UpdatedAt: time.Now().Add(-3 * time.Minute), // Updated 3 minutes ago
			},
			result: StatusFailed, // Should be considered failed due to timeout
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := tt.original
			status, err := h.GetStatus(tt.since)
			require.NoError(t, err)
			require.Equal(t, tt.result, status)
		})
	}
}
