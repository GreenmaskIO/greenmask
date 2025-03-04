package heartbeat

import (
	"context"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/storages"
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
	// TODO: Limit the max read bytes
	data, err := io.ReadAll(obj)
	if err != nil {
		return "", fmt.Errorf("read object: %w", err)
	}
	s := Status(data)
	if err := s.Validate(); err != nil {
		return "", fmt.Errorf("validate status: %w", err)
	}
	return s, nil
}
