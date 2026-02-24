package dumpstatus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/cmd"
	"github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/storages"
)

const (
	DoneStatusName            = "done"
	UnknownOrFailedStatusName = "unknown or failed"
	FailedStatusName          = "failed"
	InProgressStatusName      = "in progress"
)

const failedTimeoutMultiplayer time.Duration = 2

const heartBeatDoneContent = "done"

func GetDumpStatusAndMetadata(ctx context.Context, st storages.Storager) (string, *storage.Metadata, error) {
	objectInfo, err := st.Stat(cmd.HeartBeatFileName)
	if err != nil {
		return "", nil, err
	}
	if !objectInfo.Exist {
		// The logic for legacy
		// 1. Check metadata exist
		// 2. Open if exist and return MD and status Done
		exist, err := isMetadataExist(st)
		if err != nil {
			return "", nil, err
		}
		if !exist {
			return UnknownOrFailedStatusName, nil, nil
		}
		md, err := getMetadata(ctx, st)
		if err != nil {
			if errors.Is(err, storages.ErrFileNotFound) {
				return UnknownOrFailedStatusName, nil, nil
			}
			return "", nil, fmt.Errorf("failed to get metadata: %w", err)
		}
		return heartBeatDoneContent, md, nil
	}

	f, err := st.GetObject(ctx, cmd.HeartBeatFileName)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get heart beat file: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close heart beat file")
		}
	}()
	data, err := io.ReadAll(f)
	if err != nil {
		return "", nil, fmt.Errorf("failed to read heart beat file: %w", err)
	}
	if len(data) == 0 {
		return FailedStatusName, nil, nil
	}
	switch string(data) {
	case cmd.HeartBeatDoneContent:
		// if done - read and parse metadata
		md, err := getMetadata(ctx, st)
		if err != nil {
			if errors.Is(err, storages.ErrFileNotFound) {
				return UnknownOrFailedStatusName, nil, nil
			}
			return "", nil, fmt.Errorf("failed to get metadata: %w", err)
		}
		return heartBeatDoneContent, md, nil
	case cmd.HeartBeatInProgressContent:
		if time.Now().After(objectInfo.LastModified.Add(cmd.HeartBeatWriteInterval * failedTimeoutMultiplayer)) {
			return FailedStatusName, nil, nil
		}
		return InProgressStatusName, nil, nil
	}
	return UnknownOrFailedStatusName, nil, nil
}

func isMetadataExist(st storages.Storager) (bool, error) {
	stat, err := st.Stat(cmd.MetadataJsonFileName)
	if err != nil {
		return false, err
	}
	return stat.Exist, nil
}

func getMetadata(ctx context.Context, st storages.Storager) (*storage.Metadata, error) {
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
