package mysql

import (
	"context"
	"fmt"

	commondump "github.com/greenmaskio/greenmask/v1/internal/common/datadump"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/registry"
	utils2 "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/config"
)

func RunDump(ctx context.Context, cfg *config.Config) error {
	st, err := commondump.GetStorage(ctx, cfg)
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}

	if err := utils2.SetDefaultContextLogger(cfg.Log.Level, cfg.Log.Format); err != nil {
		return fmt.Errorf("init logger: %w", err)
	}

	dump, err := NewDump2(ctx, cfg, registry.DefaultTransformerRegistry, st)
	if err != nil {
		return fmt.Errorf("init dump process: %w", err)
	}
	if err := dump.Run(ctx); err != nil {
		return fmt.Errorf("run dump process: %w", err)
	}
	return nil
}
