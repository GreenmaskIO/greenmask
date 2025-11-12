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
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
)

type Reader struct {
	st interfaces.Storager
}

func NewReader(st interfaces.Storager) *Reader {
	return &Reader{st: st}
}

func (r *Reader) Read(ctx context.Context) (Status, error) {
	obj, err := r.st.GetObject(ctx, FileName)
	if err != nil {
		return "", fmt.Errorf("get object: %w", err)
	}
	defer func(obj io.ReadCloser) {
		err := obj.Close()
		if err != nil {
			log.Warn().
				Str("Component", "HeartbeatReader").
				Err(err).
				Msg("cannot close object")
		}
	}(obj)
	var res Heartbeat
	if err := json.NewDecoder(obj).Decode(&res); err != nil {
		return "", fmt.Errorf("decode heartbeat: %w", err)
	}
	actualStatus, err := res.GetStatus(heartBeatWriteInterval)
	if err != nil {
		return "", fmt.Errorf("get status from heartbeat: %w", err)
	}
	return actualStatus, nil
}
