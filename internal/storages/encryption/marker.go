// Copyright 2026 Greenmask
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

package encryption

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/greenmaskio/storages"
)

// MarkerFileName is the one plaintext object of an encrypted dump.
const MarkerFileName = "encryption.json"

// Marker names the public key a dump is encrypted to.
type Marker struct {
	Recipient string `json:"recipient"`
}

func getMarker(ctx context.Context, st storages.Storager) (marker Marker, found bool, err error) {
	body, err := st.GetObject(ctx, MarkerFileName)
	if errors.Is(err, storages.ErrFileNotFound) {
		return Marker{}, false, nil
	}
	if err != nil {
		return Marker{}, false, fmt.Errorf("open %s: %w", MarkerFileName, err)
	}
	data, err := io.ReadAll(body)
	if closeErr := body.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return Marker{}, false, fmt.Errorf("read %s: %w", MarkerFileName, err)
	}
	if err = json.Unmarshal(data, &marker); err != nil {
		return Marker{}, false, fmt.Errorf("decode %s: %w", MarkerFileName, err)
	}
	if marker.Recipient == "" {
		return Marker{}, false, fmt.Errorf("decode %s: no recipient", MarkerFileName)
	}
	return marker, true, nil
}

func putMarker(ctx context.Context, st storages.Storager, marker Marker) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode %s: %w", MarkerFileName, err)
	}
	if err = st.PutObject(ctx, MarkerFileName, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write %s: %w", MarkerFileName, err)
	}
	return nil
}
