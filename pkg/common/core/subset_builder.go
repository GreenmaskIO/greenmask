package core

import (
	"context"
)

type SubsetBuilder interface {
	BuildSubset(ctx context.Context, in SubsetBuilderInput) (SubsetResult, error)
}
