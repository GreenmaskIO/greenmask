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

package metadata

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

func ReadMetadata(
	ctx context.Context,
	st interfaces.Storager,
) (commonmodels.Metadata, error) {
	f, err := st.GetObject(ctx, metadataFileName)
	if err != nil {
		return commonmodels.Metadata{}, fmt.Errorf("get metadata object: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to close metadata file")
		}
	}()

	var meta commonmodels.Metadata
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return commonmodels.Metadata{}, fmt.Errorf("decode json metadata: %w", err)
	}
	return meta, nil
}
