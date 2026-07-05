package core

import "context"

// RestoreStorageProvisioner provisions a Storager already scoped to the given
// dumpID subdirectory.
//
// If dumpID is DumpIDLatest, the implementation resolves it to the most-recent
// dump with Done status before returning the scoped Storager.
//
// This is a separate interface from StorageProvisioner because restore must
// know the dumpID at provisioning time — storage must be pre-scoped before
// metadata is read.
type RestoreStorageProvisioner interface {
	Provision(ctx context.Context, cfg any, dumpID DumpID) (Storager, error)
}
