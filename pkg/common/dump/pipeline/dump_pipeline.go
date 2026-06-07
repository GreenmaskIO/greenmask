package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
)

type DumpPipeline struct {
	Stages DumpStages
	engine core.DBMSEngine
}

func NewDumpPipeline(stages DumpStages, engine core.DBMSEngine) *DumpPipeline {
	return &DumpPipeline{Stages: stages, engine: engine}
}

const (
	defaultSessionCloseTimeout = 5 * time.Second
)

func (p *DumpPipeline) NewRun(cfg config.Config) *RunState {
	state := NewRunState(cfg)
	return state
}

func (p *DumpPipeline) OpenRuntime(
	ctx context.Context,
	cfg config.Config,
) (*Runtime, error) {
	cc, err := p.Stages.ConnectionConfigurerBuilder.Build(cfg)
	if err != nil {
		return nil, fmt.Errorf("build connection configurer: %w", err)
	}
	session, err := p.Stages.DumpSessionBuilder.Open(ctx, cc)
	if err != nil {
		return nil, fmt.Errorf("open dump session: %w", err)
	}
	return &Runtime{Session: session}, nil
}

// Discover requires live DB access and runtime session.
func (p *DumpPipeline) Discover(
	ctx context.Context,
	runtime *Runtime,
	state *RunState,
) error {
	if runtime == nil || runtime.Session == nil {
		return fmt.Errorf("runtime session is required for discovery")
	}

	introspection, err := p.Stages.Introspector.Introspect(ctx, runtime.Session)
	if err != nil {
		return fmt.Errorf("introspect: %w", err)
	}
	state.Discovery.Introspection = &introspection

	graph, err := p.Stages.DependencyGraphBuilder.BuildGraph(ctx, introspection)
	if err != nil {
		return fmt.Errorf("build dependency graph: %w", err)
	}
	state.Discovery.DependencyGraph = &graph

	previousMetadata, err := p.Stages.DumpMetadataLoader.LoadPrevious(ctx, core.PreviousMetadataLoadInput{
		Engine: p.engine,
	})
	switch {
	case errors.Is(err, core.ErrPreviousMetadataNotFound):
		// No previous run to compare against — treat as a first run.
		previousMetadata = nil
	case err != nil:
		return fmt.Errorf("load previous metadata: %w", err)
	}
	state.Discovery.PreviousMetadata = previousMetadata

	if previousMetadata != nil {
		schemaDrift := p.Stages.SchemaDriftValidator.Compare(ctx, core.SchemaDriftValidatorInput{
			Previous: previousMetadata.Introspection,
			Current:  introspection,
		})
		state.Discovery.SchemaDrift = &schemaDrift
	}

	subset, err := p.Stages.SubsetBuilder.BuildSubset(ctx, core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: graph,
	})
	if err != nil {
		return fmt.Errorf("build subset: %w", err)
	}
	state.Discovery.Subset = &subset

	state.MarkExecuted(StageNameDiscovery)
	return nil
}

// BuildContext builds runtime-oriented dump context.
// It does not require a live DB session, but the resulting DumpContext
// may contain in-memory initialized objects and should not be treated
// as a durable cross-process artifact.
func (p *DumpPipeline) BuildContext(
	ctx context.Context,
	state *RunState,
) error {
	if err := state.Require(StageNameDiscovery); err != nil {
		return fmt.Errorf("check requirements: %w", err)
	}
	discoveryArtefacts := state.Discovery

	var schemaDrift core.SchemaDriftResult
	if discoveryArtefacts.SchemaDrift != nil {
		schemaDrift = *discoveryArtefacts.SchemaDrift
	}

	editedCfg := p.Stages.ConfigEditor.EditConfig(ctx, core.ConfigEditInput{
		Config:      state.Discovery.Config.Dump.Transformation.ToTransformationConfig(),
		SchemaDrift: &schemaDrift,
	})
	filterConfig, err := p.Stages.FilterConfigBuilder.Build(*discoveryArtefacts.Config)
	if err != nil {
		return fmt.Errorf("build filter config: %w", err)
	}
	filterResult, err := p.Stages.ObjectFilter.FilterObjects(ctx, core.ObjectFilterInput{
		IntrospectionResult: *discoveryArtefacts.Introspection,
		FilterConfig:        filterConfig,
	})
	if err != nil {
		return fmt.Errorf("filter objects: %w", err)
	}
	explicitCtxIn := core.ExplicitDumpContextInput{
		Config:              discoveryArtefacts.Config,
		TableConfigs:        editedCfg,
		IntrospectionResult: *discoveryArtefacts.Introspection,
		AllowedObjects:      filterResult.AllowedObjects,
		Subset:              *discoveryArtefacts.Subset,
		SchemaDrift:         schemaDrift,
	}
	explicitCtx, err := p.Stages.ExplicitDumpContextBuilder.BuildDumpContext(ctx, explicitCtxIn)
	if err != nil {
		return fmt.Errorf("build explicit dump context: %w", err)
	}
	finalCtx, err := p.Stages.DerivedDumpContextBuilder.BuildDumpContext(ctx, core.DerivedDumpContextInput{
		Config:                discoveryArtefacts.Config,
		TableConfigs:          editedCfg,
		IntrospectionResult:   *discoveryArtefacts.Introspection,
		AllowedObjects:        filterResult.AllowedObjects,
		Subset:                *discoveryArtefacts.Subset,
		SchemaDrift:           schemaDrift,
		DependencyGraphResult: *discoveryArtefacts.DependencyGraph,
		ExplicitCtx:           explicitCtx,
	})
	if err != nil {
		return fmt.Errorf("build derived dump context: %w", err)
	}

	state.Context = ContextStageArtifacts{
		EditedConfig:       editedCfg,
		ObjectFilterResult: filterResult,
		ExplicitCtx:        &explicitCtx,
		FinalCtx:           &finalCtx,
	}
	state.MarkExecuted(StageNameContextBuilding)
	return nil
}

// Snapshot and diff generation is durable/pure.
func (p *DumpPipeline) BuildSnapshotAndDiff(
	ctx context.Context,
	state *RunState,
) error {
	if err := state.Require(StageNameContextBuilding); err != nil {
		return fmt.Errorf("check requirements: %w", err)
	}
	discoveryStageArtifacts := state.Discovery
	contextStageArtifacts := state.Context
	currentSnapshot, err := p.Stages.DumpContextSnapshotBuilder.Build(ctx, *contextStageArtifacts.FinalCtx)
	if err != nil {
		return fmt.Errorf("build dump context snapshot: %w", err)
	}

	var previousSnapshot *core.DumpContextSnapshot
	if discoveryStageArtifacts.PreviousMetadata != nil {
		previousSnapshot = &discoveryStageArtifacts.PreviousMetadata.DumpContextSnapshot
	}

	diff, err := p.Stages.DumpContextDiffer.Diff(ctx, core.DumpContextDiffInput{
		Previous: previousSnapshot,
		Current:  currentSnapshot,
	})
	if err != nil {
		return fmt.Errorf("diff dump context: %w", err)
	}
	state.BuildSnapshotAndDiff = BuildSnapshotAndDiffArtifacts{
		DumpContextSnapshot: &currentSnapshot,
		DumpContextDiff:     &diff,
	}
	state.MarkExecuted(StageNameSnapshotDiffBuilding)
	return nil
}

func (p *DumpPipeline) ValidateContext(
	ctx context.Context,
	state *RunState,
) error {
	// SnapshotDiffBuilding is required as well: the validator consumes the
	// DumpContextDiff it produces, so gating on ContextBuilding alone would let a
	// caller reach a nil-dereference instead of a clean requirements error.
	if err := state.Require(StageNameContextBuilding, StageNameSnapshotDiffBuilding); err != nil {
		return fmt.Errorf("check requirements: %w", err)
	}

	buildSnapshotAndDiff := state.BuildSnapshotAndDiff

	if err := p.Stages.DumpContextValidator.Validate(ctx, core.DumpContextValidatorInput{
		DumpContext: *state.Context.FinalCtx,
		Diff:        *buildSnapshotAndDiff.DumpContextDiff,
	}); err != nil {
		return fmt.Errorf("validate dump context: %w", err)
	}

	state.MarkExecuted(StageNameContextValidation)
	return nil
}

// Plan assembly is durable/pure.
func (p *DumpPipeline) BuildPlan(
	ctx context.Context,
	state *RunState,
) error {
	if err := state.Require(
		StageNameSnapshotDiffBuilding,
		StageNameContextValidation,
	); err != nil {
		return fmt.Errorf("check requirements: %w", err)
	}

	contextBuildingArtefacts := state.Context
	discoveryArtefacts := state.Discovery

	restorationCtx, err := p.Stages.RestorationContextBuilder.Build(ctx, core.RestorationContextInput{
		DumpContext:     *contextBuildingArtefacts.FinalCtx,
		DependencyGraph: *discoveryArtefacts.DependencyGraph,
	})
	if err != nil {
		return fmt.Errorf("build restoration context: %w", err)
	}

	discoveryStageArtifacts := state.Discovery
	contextStageArtifacts := state.Context
	buildSnapshotAndDiffArtifacts := state.BuildSnapshotAndDiff
	plan, err := p.Stages.DumpPlanAssembler.Assemble(ctx, core.DumpPlanInput{
		DumpContext:         *contextStageArtifacts.FinalCtx,
		DumpContextSnapshot: *buildSnapshotAndDiffArtifacts.DumpContextSnapshot,
		DumpContextDiff:     *buildSnapshotAndDiffArtifacts.DumpContextDiff,
		RestorationContext:  restorationCtx,
		IntrospectionResult: *discoveryStageArtifacts.Introspection,
		Config:              discoveryStageArtifacts.Config.Dump.Transformation.ToTransformationConfig(),
	})
	if err != nil {
		return fmt.Errorf("assemble dump plan: %w", err)
	}

	state.BuildPlan = BuildPlanArtifacts{
		Plan: &plan,
	}
	state.MarkExecuted(StageNamePlanBuilding)
	return nil
}

func (p *DumpPipeline) ValidatePlan(
	ctx context.Context,
	state *RunState,
) error {
	if err := state.Require(StageNamePlanBuilding); err != nil {
		return fmt.Errorf("check requirements: %w", err)
	}
	if err := p.Stages.DumpPlanValidator.Validate(ctx, core.DumpPlanValidationInput{
		Plan: *state.BuildPlan.Plan,
	}); err != nil {
		return fmt.Errorf("validate dump plan: %w", err)
	}

	state.MarkExecuted(StageNamePlanValidation)
	return nil

}

// Execution requires runtime/session.
func (p *DumpPipeline) Execute(
	ctx context.Context,
	runtime *Runtime,
	state *RunState,
	opts ...core.DumpProcessorOption,
) error {
	if err := state.Require(StageNamePlanValidation); err != nil {
		return fmt.Errorf("check requirements: %w", err)
	}
	buildPlanArtefacts := state.BuildPlan
	metadata, err := p.Stages.DumpProcessor.Run(ctx, runtime.Session, *buildPlanArtefacts.Plan, opts...)
	if err != nil {
		return fmt.Errorf("dump processor: %w", err)
	}
	state.ExecuteStage = ExecuteStageArtifacts{
		Metadata: &metadata,
	}
	state.MarkExecuted(StageNameExecution)
	return nil
}

// withRuntime opens a Runtime, calls fn with it, then closes the session
// regardless of the outcome. It is the shared lifecycle wrapper used by all
// Run* methods that need a live DB connection scoped to a single operation.
//
// The deferred close intentionally uses context.Background() with a fixed
// timeout rather than ctx, so cancellation of the operation context does not
// prevent the session from being cleaned up.
//
// StageNameSessionInitialization is recorded on the state after a successful
// open, before fn is called.
//
// Callers whose session lifetime cannot be scoped to a single function call
// (e.g. Temporal workflow activities in gm-backend) should call OpenRuntime
// and runtime.Close directly instead.
func (p *DumpPipeline) withRuntime(
	ctx context.Context,
	cfg config.Config,
	state *RunState,
	fn func(runtime *Runtime) error,
) error {
	runtime, err := p.OpenRuntime(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open runtime: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultSessionCloseTimeout)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			log.Ctx(closeCtx).Err(err).Msg("close runtime object")
		}
	}()
	state.MarkExecuted(StageNameSessionInitialization)
	return fn(runtime)
}

func (p *DumpPipeline) RunDump(ctx context.Context, cfg config.Config, opts ...core.DumpProcessorOption) (*RunState, error) {
	state := p.NewRun(cfg)
	if err := p.withRuntime(ctx, cfg, state, func(runtime *Runtime) error {
		if err := p.Discover(ctx, runtime, state); err != nil {
			return fmt.Errorf("discovery stage: %w", err)
		}
		if err := p.BuildContext(ctx, state); err != nil {
			return fmt.Errorf("build context stage: %w", err)
		}
		if err := p.BuildSnapshotAndDiff(ctx, state); err != nil {
			return fmt.Errorf("build snapshot and diff stage: %w", err)
		}
		if err := p.ValidateContext(ctx, state); err != nil {
			return fmt.Errorf("validate context stage: %w", err)
		}
		if err := p.BuildPlan(ctx, state); err != nil {
			return fmt.Errorf("build plan stage: %w", err)
		}
		if err := p.ValidatePlan(ctx, state); err != nil {
			return fmt.Errorf("validate plan stage: %w", err)
		}
		return p.Execute(ctx, runtime, state, opts...)
	}); err != nil {
		return nil, err
	}
	return state, nil
}

func (p *DumpPipeline) RunValidateConfig(ctx context.Context, cfg config.Config) (*RunState, error) {
	state := p.NewRun(cfg)
	if err := p.withRuntime(ctx, cfg, state, func(runtime *Runtime) error {
		if err := p.Discover(ctx, runtime, state); err != nil {
			return fmt.Errorf("discovery stage: %w", err)
		}
		return p.BuildContext(ctx, state)
	}); err != nil {
		return nil, err
	}
	return state, nil
}

// RunValidateContext runs snapshot+diff building and context validation against
// an already-built state (discovery and context building must have completed).
func (p *DumpPipeline) RunValidateContext(ctx context.Context, state *RunState) error {
	if err := state.Require(StageNameContextBuilding); err != nil {
		return fmt.Errorf("check requirements: %w", err)
	}

	if err := p.BuildSnapshotAndDiff(ctx, state); err != nil {
		return fmt.Errorf("build snapshot and diff stage: %w", err)
	}

	if err := p.ValidateContext(ctx, state); err != nil {
		return fmt.Errorf("validate context stage: %w", err)
	}
	return nil
}

// RunValidatePlan runs the full planning pipeline (snapshot+diff, context
// validation, plan assembly, plan validation) against an already-built state.
func (p *DumpPipeline) RunValidatePlan(ctx context.Context, state *RunState) error {
	if err := state.Require(StageNameContextBuilding); err != nil {
		return fmt.Errorf("check requirements: %w", err)
	}

	if err := p.BuildSnapshotAndDiff(ctx, state); err != nil {
		return fmt.Errorf("build snapshot and diff stage: %w", err)
	}

	if err := p.ValidateContext(ctx, state); err != nil {
		return fmt.Errorf("validate context stage: %w", err)
	}

	if err := p.BuildPlan(ctx, state); err != nil {
		return fmt.Errorf("build plan stage: %w", err)
	}

	if err := p.ValidatePlan(ctx, state); err != nil {
		return fmt.Errorf("validate plan stage: %w", err)
	}
	return nil
}

func (p *DumpPipeline) RunShowSchemaDrift(ctx context.Context, cfg config.Config) (*RunState, error) {
	state := p.NewRun(cfg)
	if err := p.withRuntime(ctx, cfg, state, func(runtime *Runtime) error {
		return p.Discover(ctx, runtime, state)
	}); err != nil {
		return state, err
	}
	return state, nil
}

// RunShowDumpDiff runs discovery, context building, and snapshot+diff generation,
// returning the state so callers can inspect the DumpContextDiff artifact.
func (p *DumpPipeline) RunShowDumpDiff(ctx context.Context, cfg config.Config) (*RunState, error) {
	state := p.NewRun(cfg)
	if err := p.withRuntime(ctx, cfg, state, func(runtime *Runtime) error {
		if err := p.Discover(ctx, runtime, state); err != nil {
			return fmt.Errorf("discovery stage: %w", err)
		}
		if err := p.BuildContext(ctx, state); err != nil {
			return fmt.Errorf("build context stage: %w", err)
		}
		return p.BuildSnapshotAndDiff(ctx, state)
	}); err != nil {
		return nil, err
	}
	return state, nil
}
