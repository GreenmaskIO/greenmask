package core

import (
	"context"
)

type TransformerProvisioner interface {
	Init(
		ctx context.Context,
		driver TableDriver,
		config TransformerConfig,
	) (TransformerContexter, error)
}
