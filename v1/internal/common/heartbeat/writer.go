package heartbeat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const (
	heartBeatFileName = "heartbeat"
)

type Writer struct {
	st storages.Storager
}

func NewWriter(st storages.Storager) *Writer {
	return &Writer{st: st}
}

func (w *Writer) Write(ctx context.Context, s Status) error {
	if err := s.Validate(); err != nil {
		return fmt.Errorf("validate status: %w", err)
	}

	b := bytes.NewBuffer(nil)
	heartbeat := Heartbeat{Status: s, UpdatedAt: time.Now()}
	if err := json.NewEncoder(b).Encode(heartbeat); err != nil {
		return fmt.Errorf("encode heartbeat: %w", err)
	}
	if err := w.st.PutObject(ctx, heartBeatFileName, b); err != nil {
		return err
	}
	return nil
}
