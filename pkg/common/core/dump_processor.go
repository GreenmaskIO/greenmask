package core

import (
	"context"
)

type DumpProcessor interface {
	Run(ctx context.Context, session DumpSession, st Storager, plan DumpPlan, opts ...DumpProcessorOption) (Metadata, error)
}
