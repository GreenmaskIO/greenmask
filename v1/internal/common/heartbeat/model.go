package heartbeat

import (
	"fmt"
	"time"
)

type Heartbeat struct {
	Status    Status    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (h *Heartbeat) GetStatus(since time.Duration) (Status, error) {
	if err := h.Status.Validate(); err != nil {
		return "", fmt.Errorf("validate status: %w", err)
	}
	if h.Status == StatusDone || h.Status == StatusFailed {
		return h.Status, nil
	}

	// Now handle in progress status.
	if time.Since(h.UpdatedAt) > since {
		// If the heartbeat is in progress and the last update is older than the refresh timeout,
		// we consider it as failed.
		return StatusFailed, nil
	}
	// Otherwise, we return the current status (in progress).
	return h.Status, nil
}
