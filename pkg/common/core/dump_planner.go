package core

import (
	"context"
)

type RestorationContextBuilder interface {
	Build(ctx context.Context, input RestorationContextInput) (RestorationContext, error)
}
