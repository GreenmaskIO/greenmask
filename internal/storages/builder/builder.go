package builder

import (
	"context"
	"errors"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/greenmaskio/greenmask/internal/storages/s3"
)

func GetStorage(ctx context.Context, stCfg *domains.StorageConfig, logCgf *domains.LogConfig) (
	storages.Storager, error,
) {
	if stCfg.Directory != nil {
		return directory.NewStorage(stCfg.Directory)
	} else if stCfg.S3 != nil {
		return s3.NewStorage(ctx, stCfg.S3, logCgf.Level)
	}
	return nil, errors.New("no one storage was provided")
}
