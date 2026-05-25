package pipeline

import (
	"context"

	commonininterfaces "github.com/greenmaskio/greenmask/pkg/common/interfaces"
)

type Runtime struct {
	Session commonininterfaces.DumpSession
}

func (r *Runtime) Close(ctx context.Context) error {
	return r.Session.Close(ctx)
}
