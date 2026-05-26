package dump

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
)

type DumpSessionBuilder struct{}

func (b *DumpSessionBuilder) Open(ctx context.Context, cfg interfaces.ConnectionConfigurer) (interfaces.DumpSession, error) {
	c, ok := cfg.ConnectionConfig().(*DumpConnectionConfig)
	if !ok {
		return nil, fmt.Errorf("unexpected connection config type %T, want *DumpConnectionConfig", cfg.ConnectionConfig())
	}
	_ = c
	// TODO: build MySQL dump session using c.Common and c.MySQL
	return nil, fmt.Errorf("not implemented")
}
