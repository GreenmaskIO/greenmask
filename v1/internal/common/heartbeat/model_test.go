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
