package core

import "context"

// RestoreMetadataReader reads the dump metadata.json from a Storager that has
// already been scoped to the target dumpID subdirectory by
// RestoreStorageProvisioner.
type RestoreMetadataReader interface {
	ReadMetadata(ctx context.Context, st Storager) (Metadata, error)
}
