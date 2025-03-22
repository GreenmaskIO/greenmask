package dump

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

func GetDumpStorage(ctx context.Context, cfg *config.Config) (storages.Storager, error) {
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
	st = st.SubStorage(strconv.FormatInt(time.Now().UnixMilli(), 10), true)
	return st, nil
}

func GetContext(cfg *config.Config) (context.Context, error) {
	ctx, err := utils.GetLoggerContext(context.Background(), cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		return nil, fmt.Errorf("get logger: %w", err)
	}
	return ctx, nil
}
