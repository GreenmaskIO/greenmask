package heartbeat

import (
	"bytes"
	"context"
	"fmt"

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
	b := bytes.NewBuffer([]byte(s))
	if err := w.st.PutObject(ctx, heartBeatFileName, b); err != nil {
		return err
	}
	return nil
}
