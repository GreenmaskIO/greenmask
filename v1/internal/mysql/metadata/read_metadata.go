package metadata

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog/log"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

func ReadMetadata(
	ctx context.Context,
	st storages.Storager,
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
