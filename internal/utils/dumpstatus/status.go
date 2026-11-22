package dumpstatus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/cmd"
	"github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/storages"
)

const (
	DoneStatusName            = "done"
	UnknownOrFailedStatusName = "unknown or failed"
	FailedStatusName          = "failed"
	InProgressStatusName      = "in progress"
)

const failedTimeoutMultiplayer time.Duration = 2

// GetStatus classifies the dump by its heartbeat; it reads no encrypted object, so it needs no key.
func GetStatus(ctx context.Context, st storages.Storager) (string, error) {
	objectInfo, err := st.Stat(cmd.HeartBeatFileName)
	if err != nil {
		return "", err
	}
	if !objectInfo.Exist {
		// A legacy dump has no heartbeat: it is done when its metadata exists.
		exist, err := isMetadataExist(st)
		if err != nil {
			return "", err
		}
		if !exist {
			return UnknownOrFailedStatusName, nil
		}
		return DoneStatusName, nil
	}

	f, err := st.GetObject(ctx, cmd.HeartBeatFileName)
	if err != nil {
		return "", fmt.Errorf("failed to get heart beat file: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close heart beat file")
		}
	}()
	data, err := io.ReadAll(f)
	if err != nil {
		return "", fmt.Errorf("failed to read heart beat file: %w", err)
	}
	if len(data) == 0 {
		return FailedStatusName, nil
	}
	switch string(data) {
	case cmd.HeartBeatDoneContent:
		exist, err := isMetadataExist(st)
		if err != nil {
			return "", err
		}
		if !exist {
			return UnknownOrFailedStatusName, nil
		}
		return DoneStatusName, nil
	case cmd.HeartBeatInProgressContent:
		if time.Now().After(objectInfo.LastModified.Add(cmd.HeartBeatWriteInterval * failedTimeoutMultiplayer)) {
			return FailedStatusName, nil
		}
		return InProgressStatusName, nil
	}
	return UnknownOrFailedStatusName, nil
}

func isMetadataExist(st storages.Storager) (bool, error) {
	stat, err := st.Stat(cmd.MetadataJsonFileName)
	if err != nil {
		return false, err
	}
	return stat.Exist, nil
}

// GetMetadata reads the dump's metadata; an encrypted dump needs its key.
func GetMetadata(ctx context.Context, st storages.Storager) (*storage.Metadata, error) {
	mf, err := st.GetObject(ctx, cmd.MetadataJsonFileName)
	if err != nil {
		return nil, fmt.Errorf("get metadata from storage: %w", err)
	}
	defer func() {
		if err := mf.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close metadata file")
		}
	}()

	metadata := &storage.Metadata{}
	if err = json.NewDecoder(mf).Decode(metadata); err != nil {
		return nil, fmt.Errorf("unable to read metadata: %w", err)
	}
	return metadata, nil
}
