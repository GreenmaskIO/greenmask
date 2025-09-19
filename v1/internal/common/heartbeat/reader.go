package heartbeat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

type Reader struct {
	st storages.Storager
}

func NewReader(st storages.Storager) *Reader {
	return &Reader{st: st}
}

func (r *Reader) Read(ctx context.Context) (Status, error) {
	obj, err := r.st.GetObject(ctx, heartBeatFileName)
	if err != nil {
		return "", fmt.Errorf("get object: %w", err)
	}
	defer func(obj io.ReadCloser) {
		err := obj.Close()
		if err != nil {
			log.Warn().
				Str("Component", "HeartbeatReader").
				Err(err).
				Msg("cannot close object")
		}
	}(obj)
	var res Heartbeat
	if err := json.NewDecoder(obj).Decode(&res); err != nil {
		return "", fmt.Errorf("decode heartbeat: %w", err)
	}
	actualStatus, err := res.GetStatus(heartBeatWriteInterval)
	if err != nil {
		return "", fmt.Errorf("get status from heartbeat: %w", err)
	}
	return actualStatus, nil
}
