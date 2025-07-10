package pipeline

import (
	"context"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
)

// Dummy - it's a pipeline for raw data dumping.
type Dummy struct{}

func NewDummy() *Dummy {
	return &Dummy{}
}

func (d Dummy) Transform(_ context.Context, _ commonininterfaces.Recorder) error {
	return nil
}

func (d Dummy) Init(ctx context.Context) error {
	return nil
}

func (d Dummy) Done(ctx context.Context) error {
	return nil
}
