package dump

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/registry"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

func RunDump(ctx context.Context, cfg *config.Config, st storages.Storager) error {
	dump, err := NewDump(cfg, registry.DefaultTransformerRegistry, st)
	if err != nil {
		return fmt.Errorf("init dump process: %w", err)
	}
	if err := dump.Run(ctx); err != nil {
		return fmt.Errorf("run dump process: %w", err)
	}
	return nil
}
