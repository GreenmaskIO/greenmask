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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const (
	heartBeatFileName = "heartbeat"
)

type Writer struct {
	st storages.Storager
}

func NewWriter(st storages.Storager) *Writer {
	return &Writer{st: st}
}

func (w *Writer) Write(ctx context.Context, s Status) error {
	if err := s.Validate(); err != nil {
		return fmt.Errorf("validate status: %w", err)
	}

	b := bytes.NewBuffer(nil)
	heartbeat := Heartbeat{Status: s, UpdatedAt: time.Now()}
	if err := json.NewEncoder(b).Encode(heartbeat); err != nil {
		return fmt.Errorf("encode heartbeat: %w", err)
	}
	if err := w.st.PutObject(ctx, heartBeatFileName, b); err != nil {
		return err
	}
	return nil
}
