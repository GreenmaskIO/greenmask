// Package storage provides engine-agnostic helpers for resolving a dumpID to
// a Storager scoped to that dump's subdirectory.
package storage

import (
	"context"
	"fmt"
	"path"
	"slices"
	"time"

	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	heartbeat "github.com/greenmaskio/greenmask/pkg/common/heartbeat"
)

var (
	ErrDumpIDNotFound = fmt.Errorf("dump with provided id is not found")
	ErrNoLatestDumpID = fmt.Errorf("no dumps found with done status, please create a dump first")
)

const metadataJSONFileName = "metadata.json"

func getDumpStatus(
	ctx context.Context,
	st core.Storager,
	dumpID core.DumpID,
	staleTimeout time.Duration,
) (heartbeat.Status, error) {
	subSt := st.SubStorage(string(dumpID), true)
	status, err := heartbeat.NewReader(subSt).SetStaleTimeout(staleTimeout).Read(ctx)
	if err != nil {
		return "", fmt.Errorf("read heartbeat file: %w", err)
	}
	return status, nil
}

func GetLatestDumpID(ctx context.Context, st core.Storager, staleTimeout time.Duration) (core.DumpID, error) {
	var dumpIDs []core.DumpID

	_, dirs, err := st.ListDir(ctx)
	if err != nil {
		return "", fmt.Errorf("list storage directory: %w", err)
	}
	for _, dir := range dirs {
		exists, err := dir.Exists(ctx, heartbeat.FileName)
		if err != nil {
			return "", fmt.Errorf("check heartbeat file existence: %w", err)
		}
		if exists {
			dumpIDs = append(dumpIDs, core.DumpID(dir.Dirname()))
		}
	}

	slices.SortFunc(dumpIDs, func(a, b core.DumpID) int {
		if a > b {
			return -1
		}
		return 1
	})

	for _, dumpID := range dumpIDs {
		status, err := getDumpStatus(ctx, st, dumpID, staleTimeout)
		if err != nil {
			return "", fmt.Errorf("get dump status for dumpID=%s: %w", dumpID, err)
		}
		if status == heartbeat.StatusDone {
			log.Ctx(ctx).Info().Any("dumpID", dumpID).Msg("found latest dumpID")
			return dumpID, nil
		}
	}
	return "", ErrNoLatestDumpID
}

func VerifyConcreteDumpID(ctx context.Context, st core.Storager, dumpID core.DumpID) (core.DumpID, error) {
	exists, err := st.Exists(ctx, path.Join(string(dumpID), metadataJSONFileName))
	if err != nil {
		return "", fmt.Errorf("check dumpID=%s exists: %w", dumpID, err)
	}
	if !exists {
		return "", fmt.Errorf("check dumpID=%s: %w", dumpID, ErrDumpIDNotFound)
	}
	return dumpID, nil
}

// GetStorageByDumpID resolves dumpID (possibly "latest") to a concrete dump ID
// and returns a Storager scoped to that dump's subdirectory.
func GetStorageByDumpID(
	ctx context.Context,
	st core.Storager,
	dumpID core.DumpID,
	staleTimeout time.Duration,
) (core.Storager, error) {
	var err error
	if dumpID == core.DumpIDLatest {
		dumpID, err = GetLatestDumpID(ctx, st, staleTimeout)
		if err != nil {
			return nil, fmt.Errorf("get latest dumpID: %w", err)
		}
	} else {
		dumpID, err = VerifyConcreteDumpID(ctx, st, dumpID)
		if err != nil {
			return nil, fmt.Errorf("verify concrete dumpID: %w", err)
		}
	}
	return st.SubStorage(string(dumpID), true), nil
}
