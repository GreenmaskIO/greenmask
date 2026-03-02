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
