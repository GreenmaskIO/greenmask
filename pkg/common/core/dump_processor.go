package core

import (
	"context"
)

type DumpProcessor interface {
	Run(ctx context.Context, session DumpSession, conn ConnectionConfigurer, st Storager, plan DumpPlan) (Metadata, error)
}
