// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import (
	"context"
	"fmt"
	"path"
	"slices"

	cmdInternals "github.com/greenmaskio/greenmask/internal/db/postgres/cmd"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/v1/internal/common/heartbeat"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const DumpIDLatest = "latest"

var (
	errDumpIDNotFound = fmt.Errorf("dump with provided id is not found")
	errEmptyDumpID    = fmt.Errorf("dump id is empty, please provide dump id or use 'latest' to restore the latest dump")
	errNoLatestDumpID = fmt.Errorf("no dumps found with done status, please provide create a dump first")
)

func getDumpStatus(
	ctx context.Context, st storages.Storager, dumpID commonmodels.DumpID,
) (heartbeat.Status, error) {
	if dumpID == DumpIDLatest {
		return "", errEmptyDumpID
	}
	st = st.SubStorage(string(dumpID), true)
	status, err := heartbeat.NewReader(st).Read(ctx)
	if err != nil {
		return "", fmt.Errorf("read heartbeat file: %w", err)
	}
	return status, nil
}

func getLatestDumpID(ctx context.Context, st storages.Storager) (commonmodels.DumpID, error) {
	var dumpIDs []commonmodels.DumpID

	_, dirs, err := st.ListDir(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot walk through directory")
	}
	for _, dir := range dirs {
		exists, err := dir.Exists(ctx, cmdInternals.HeartBeatFileName)
		if err != nil {
			log.Fatal().Err(err).Msg("cannot check file existence")
		}
		if exists {
			dumpIDs = append(dumpIDs, commonmodels.DumpID(dir.Dirname()))
		}
	}

	slices.SortFunc(
		dumpIDs, func(a, b commonmodels.DumpID) int {
			if a > b {
				return -1
			}
			return 1
		},
	)

	for _, dumpID := range dumpIDs {
		status, err := getDumpStatus(ctx, st, dumpID)
		if err != nil {
			return "", fmt.Errorf("get dump status for dumpID=%s: %w", dumpID, err)
		}
		if status == heartbeat.StatusDone {
			log.Ctx(ctx).Info().Any("dumpID", dumpID).Msg("found latest dumpID")
			return dumpID, nil
		}
	}
	return "", errNoLatestDumpID
}

func verifyConcreteDumpID(
	ctx context.Context, st storages.Storager, dumpId commonmodels.DumpID,
) (commonmodels.DumpID, error) {
	exists, err := st.Exists(ctx, path.Join(string(dumpId), cmdInternals.MetadataJsonFileName))
	if err != nil {
		return "", fmt.Errorf("check dumpID=%s exists: %w", dumpId, err)
	}
	if !exists {
		return "", fmt.Errorf("check dumpID=%s exists: %w", dumpId, errDumpIDNotFound)
	}
	return dumpId, nil
}

func getStorageByDumpID(
	ctx context.Context, st storages.Storager, dumpID commonmodels.DumpID,
) (storages.Storager, error) {
	var err error
	actualDumpID := dumpID
	if dumpID == DumpIDLatest {
		actualDumpID, err = getLatestDumpID(ctx, st)
		if err != nil {
			return nil, fmt.Errorf("get latest dumpID: %w", err)
		}
	} else {
		actualDumpID, err = verifyConcreteDumpID(ctx, st, dumpID)
		if err != nil {
			return nil, fmt.Errorf("verify concrete dumpID: %w", err)
		}
	}
	return st.SubStorage(string(actualDumpID), true), nil
}

func RunRestore(
	ctx context.Context, cfg *config.Config, st storages.Storager, dumpIDArg string,
) error {
	dumpID := commonmodels.DumpID(dumpIDArg)
	if err := dumpID.Validate(); err != nil {
		return fmt.Errorf("validate dumpID: %w", err)
	}
	st, err := getStorageByDumpID(ctx, st, dumpID)
	if err != nil {
		return fmt.Errorf("get storage by dumpID: %w", err)
	}
	if err := NewRestore(cfg, st, dumpID).Run(ctx); err != nil {
		return fmt.Errorf("run restore process: %w", err)
	}
	return nil
}
