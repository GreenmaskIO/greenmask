package mysql

import (
	"context"
	"fmt"

	commondump "github.com/greenmaskio/greenmask/v1/internal/common/dump"
	"github.com/greenmaskio/greenmask/v1/internal/config"
)

func RunDump(ctx context.Context, cfg *config.Config) error {
	ctx, err := commondump.GetContext(cfg)
	if err != nil {
		return fmt.Errorf("get context: %w", err)
	}

	st, err := commondump.GetDumpStorage(ctx, cfg)
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}

	dump := commondump.NewDumpRuntime(nil, nil, st)

	if err := dump.Run(ctx); err != nil {
		return fmt.Errorf("run dump: %w", err)
	}
	return nil
}
