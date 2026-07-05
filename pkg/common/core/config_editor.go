package core

import (
	"context"
)

type ConfigEditor interface {
	EditConfig(ctx context.Context, input ConfigEditInput) []TableConfig
}
