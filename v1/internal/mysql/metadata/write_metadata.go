package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const metadataFileName = "metadata.json"

func WriteMetadata(
	ctx context.Context,
	st storages.Storager,
	engine string,
	transformationConfig []commonmodels.TableConfig,
	startedAt time.Time,
	completedAt time.Time,
	dumpStats commonmodels.DumpStat,
	tables []commonmodels.Table,
) error {
	meta := commonmodels.NewMetadata(
		engine,
		dumpStats,
		startedAt,
		completedAt,
		transformationConfig,
		tables,
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
