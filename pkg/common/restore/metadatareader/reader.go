// Package metadatareader provides a RestoreMetadataReader implementation that
// reads dump metadata from a pre-scoped Storager, analogous to
// pkg/common/dump/metadatawriter for the write side.
package metadatareader

import (
	"context"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/metadata"
)

// Reader implements core.RestoreMetadataReader using the shared
// metadata.ReadMetadata function.
type Reader struct{}

func New() *Reader { return &Reader{} }

func (r *Reader) ReadMetadata(ctx context.Context, st core.Storager) (core.Metadata, error) {
	return metadata.ReadMetadata(ctx, st)
}
