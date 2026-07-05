package core

import (
	"context"
)

type DumpMetadataLoader interface {
	LoadPrevious(ctx context.Context, input PreviousMetadataLoadInput) (*Metadata, error)
}
