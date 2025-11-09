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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const metadataFileName = "metadata.json"

func WriteMetadata(
	ctx context.Context,
	st storages.Storager,
	engine string,
	cfg config.Dump,
	startedAt time.Time,
	completedAt time.Time,
	dumpStats commonmodels.DumpStat,
	tables []commonmodels.Table,
	databaseName string,
) error {
	meta := commonmodels.NewMetadata(
		engine,
		dumpStats,
		startedAt,
		completedAt,
		cfg.Transformation.ToTransformationConfig(),
		tables,
		databaseName,
		cfg.Tag,
		cfg.Description,
	)
	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(meta); err != nil {
		return fmt.Errorf("encode json metadata: %w", err)
	}
	if err := st.PutObject(ctx, metadataFileName, buf); err != nil {
		return fmt.Errorf("put metadata object: %w", err)
	}
	return nil
}
