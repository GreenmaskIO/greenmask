package core

import "context"

// RestoreInstruction carries execution-time parameters for the restore
// processor. It is produced by RestoreInstructionBuilder just before execution.
type RestoreInstruction struct {
	Jobs           int
	DataOnly       bool
	SchemaOnly     bool
	RestoreInOrder bool
	Section        []string
	Scripts        []Script
}

// RestoreInstructionBuilder extracts restore execution parameters from config.
//
// Build receives the full config as any to avoid an import cycle (pkg/config
// already imports this package). Implementations type-assert to config.Config
// internally.
type RestoreInstructionBuilder interface {
	Build(ctx context.Context, cfg any) (RestoreInstruction, error)
}
