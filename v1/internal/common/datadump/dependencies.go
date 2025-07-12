package datadump

import (
	"context"

	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

func GetStorage(ctx context.Context, cfg *config.Config) (storages.Storager, error) {
	st, err := storages.Get(
		ctx,
		cfg.Storage.Type,
		cfg.Storage.S3.ToS3Config(),
		cfg.Storage.Directory.ToDirectoryConfig(),
		cfg.Log.Level,
	)
	if err != nil {
		return nil, err
	}
	return st, nil
}
