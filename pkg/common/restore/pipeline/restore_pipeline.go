package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
)

const defaultRestoreSessionCloseTimeout = 5 * time.Second

// RestorePipeline orchestrates the restore operation for a single DBMS engine.
//
// It is the restore-side analogue of DumpPipeline. Each stage in RestoreStages
// is a pluggable interface; the pipeline itself is engine-agnostic.
type RestorePipeline struct {
	Stages RestoreStages
	engine core.DBMSEngine
}

// NewRestorePipeline returns a RestorePipeline wired with the supplied stages
// and engine identifier.
func NewRestorePipeline(stages RestoreStages, engine core.DBMSEngine) *RestorePipeline {
	return &RestorePipeline{Stages: stages, engine: engine}
}

// OpenRuntime builds a ConnectionConfigurer from cfg and opens a DatabaseSession
// to the target DB.
//
// Callers whose session lifetime cannot be scoped to a single function call
// (e.g. Temporal workflow activities in gm-backend) should call OpenRuntime and
// runtime.Close directly instead of RunRestore.
func (p *RestorePipeline) OpenRuntime(
	ctx context.Context,
	cfg config.Config,
) (*RestoreRuntime, error) {
	cc, err := p.Stages.ConnectionConfigurerBuilder.Build(cfg)
	if err != nil {
		return nil, fmt.Errorf("build connection configurer: %w", err)
	}
	session, err := p.Stages.DatabaseSessionBuilder.Open(ctx, cc)
	if err != nil {
		return nil, fmt.Errorf("open restore session: %w", err)
	}
	return &RestoreRuntime{Session: session}, nil
}

// Execute runs the three steps of the restore execution phase:
//
//  1. Provision storage scoped to the resolved dumpID.
//  2. Read dump metadata from that storage.
//  3. Build the restore instruction and run the restore processor.
//
// Execute requires RestoreStageNameSessionInitialization to have been marked on
// state (i.e. withRuntime must have been called first).
func (p *RestorePipeline) Execute(
	ctx context.Context,
	runtime *RestoreRuntime,
	state *RestoreRunState,
) error {
	if err := state.Require(RestoreStageNameSessionInitialization); err != nil {
		return fmt.Errorf("check requirements: %w", err)
	}

	st, err := p.Stages.RestoreStorageProvisioner.Provision(ctx, state.Config, state.DumpID)
	if err != nil {
		return fmt.Errorf("provision restore storage: %w", err)
	}
	state.Storage = st

	meta, err := p.Stages.RestoreMetadataReader.ReadMetadata(ctx, st)
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}
	state.Metadata = &meta

	instr, err := p.Stages.RestoreInstructionBuilder.Build(ctx, state.Config)
	if err != nil {
		return fmt.Errorf("build restore instruction: %w", err)
	}

	// Build connection configurer a second time so the processor has access to
	// vendor-specific parameters for CLI subprocess invocations (e.g. mysql CLI
	// for schema restore) and per-table data connections.
	conn, err := p.Stages.ConnectionConfigurerBuilder.Build(state.Config)
	if err != nil {
		return fmt.Errorf("build connection configurer for execution: %w", err)
	}

	if err := p.Stages.RestoreProcessor.Run(ctx, core.RestoreRunInput{
		Session:     runtime.Session,
		Conn:        conn,
		St:          st,
		Meta:        meta,
		Instruction: instr,
	}); err != nil {
		return fmt.Errorf("restore processor: %w", err)
	}

	state.MarkExecuted(RestoreStageNameExecution)
	return nil
}

// withRuntime opens a RestoreRuntime, calls fn with it, and defers session
// close regardless of outcome. StageNameSessionInitialization is recorded on
// state before fn is called.
//
// The deferred close uses context.Background with a fixed timeout so that
// cancellation of the operation context does not prevent cleanup.
func (p *RestorePipeline) withRuntime(
	ctx context.Context,
	cfg config.Config,
	state *RestoreRunState,
	fn func(runtime *RestoreRuntime) error,
) error {
	runtime, err := p.OpenRuntime(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open runtime: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultRestoreSessionCloseTimeout)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			log.Ctx(closeCtx).Err(err).Msg("close restore runtime")
		}
	}()
	state.MarkExecuted(RestoreStageNameSessionInitialization)
	return fn(runtime)
}

// RunRestore is the main entry point for a restore operation.
//
// dumpID can be core.DumpIDLatest ("latest") or a concrete millisecond
// timestamp string produced by a previous dump.
func (p *RestorePipeline) RunRestore(
	ctx context.Context,
	cfg config.Config,
	dumpID core.DumpID,
) (*RestoreRunState, error) {
	state := NewRestoreRunState(cfg, dumpID)
	if err := p.withRuntime(ctx, cfg, state, func(runtime *RestoreRuntime) error {
		return p.Execute(ctx, runtime, state)
	}); err != nil {
		return nil, err
	}
	return state, nil
}
