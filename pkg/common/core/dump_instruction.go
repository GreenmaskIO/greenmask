package core

import (
	"context"
	"time"
)

// DumpInstruction carries execution-time parameters for the dump processor.
// It is produced by DumpInstructionBuilder just before execution and passed
// into DumpProcessor.Run alongside the plan.
type DumpInstruction struct {
	Jobs              int
	HeartbeatInterval time.Duration
}

// DumpInstructionBuilder extracts execution parameters from the dump config
// and assembles them into a DumpInstruction.
//
// Build receives the full config as any to avoid an import cycle (pkg/config
// already imports this package). Implementations type-assert to config.Config
// internally.
type DumpInstructionBuilder interface {
	Build(ctx context.Context, cfg any) (DumpInstruction, error)
}
