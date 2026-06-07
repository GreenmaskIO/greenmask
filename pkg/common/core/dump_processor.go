package core

import (
	"context"
)

type DumpProcessor interface {
	Run(ctx context.Context, session DumpSession, plan DumpPlan, opts ...DumpProcessorOption) (Metadata, error)
}
