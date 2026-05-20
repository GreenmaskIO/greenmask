package pipeline

import (
	"context"
	"fmt"
	"time"

	commonininterfaces "github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/rs/zerolog/log"
)

// DumpStages represents the complete database dump planning and execution pipeline.
//
// The pipeline transforms high-level user configuration and live database metadata
// into a validated executable dump plan and executes it.
//
// The pipeline is split into multiple logical stages:
//
//   - Introspection
//   - Dependency analysis
//   - Subset planning
//   - Schema drift analysis
//   - Configuration enrichment
//   - Dump context generation
//   - Semantic derivation
//   - Validation and diffing
//   - Restoration planning
//   - Dump plan assembly
//   - Execution
//
// The resulting DumpPlan represents an immutable executable snapshot
// of the planned dump operation.
type DumpStages struct {
	DumpSessionBuilder commonininterfaces.DumpSessionBuilder

	// Introspector performs database schema introspection and collects
	// runtime metadata required for dump planning.
	//
	// Examples:
	//   - tables
	//   - columns
	//   - constraints
	//   - indexes
	//   - sequences
	//   - extensions
	//   - object ownership
	Introspector commonininterfaces.IntrospectorV2

	// DependencyGraphBuilder constructs the object dependency graph
	// from introspection results.
	//
	// The graph contains:
	//   - object-level dependencies
	//   - SCC condensed graph
	//   - cycle information
	//   - semantic object links
	//
	// The graph is later used for:
	//   - subset generation
	//   - transformation propagation
	//   - restoration ordering
	//   - integrity validation
	DependencyGraphBuilder commonininterfaces.DependencyGraphBuilder

	DumpMetadataLoader commonininterfaces.DumpMetadataLoader

	// SchemaDriftValidator analyzes differences between previous dump metadata
	// and current database introspection results.
	//
	// This stage detects:
	//   - removed objects
	//   - added objects
	//   - renamed columns
	//   - incompatible schema changes
	//   - transformation invalidation
	//
	// The result may later influence derived dump context generation.
	SchemaDriftValidator commonininterfaces.SchemaDriftValidator

	// SubsetBuilder generates subset queries and subset dependency metadata
	// using the dependency graph and configuration rules.
	//
	// This stage is responsible for planning partial data extraction
	// while preserving referential integrity.
	SubsetBuilder commonininterfaces.SubsetBuilder

	// ConfigEditor updates and enriches the original user configuration
	// using additional semantic information.
	//
	// Examples:
	//   - classification-driven rules
	//   - policy-generated transformations
	//   - inherited defaults
	//   - auto-generated include/exclude rules
	ConfigEditor commonininterfaces.ConfigEditor

	// ExplicitDumpContextBuilder constructs the initial dump context
	// directly from explicit user configuration and introspection results.
	//
	// This stage produces:
	//   - explicit transformers
	//   - explicit dump object specs
	//   - schema dump specs
	//   - initial runtime payloads
	ExplicitDumpContextBuilder commonininterfaces.ExplicitDumpContextBuilder

	// DerivedDumpContextBuilder enriches the dump context using
	// semantic derivation and dependency analysis.
	//
	// Examples:
	//   - PK -> FK transformation propagation
	//   - implicit transformations
	//   - subset-aware reconciliation
	//   - schema drift adaptation
	//   - semantic inheritance
	//
	// This stage produces the final semantic dump context.
	DerivedDumpContextBuilder commonininterfaces.DerivedDumpContextBuilder

	DumpContextSnapshotBuilder commonininterfaces.DumpContextSnapshotBuilder

	// DumpContextDiffer compares dump contexts and produces
	// deterministic semantic diffs.
	//
	// This stage may be used for:
	//   - GitOps workflows
	//   - approval pipelines
	//   - UI visualization
	//   - audit history
	//   - dry-run analysis
	// TODO: 2026-05-19 I've stopped here
	DumpContextDiffer commonininterfaces.DumpContextDiffer

	// DumpContextValidator validates semantic correctness
	// of the final dump context.
	//
	// Examples:
	//   - invalid transformer propagation
	//   - unresolved semantic conflicts
	//   - incompatible transformations
	//   - missing referenced objects
	//   - unsupported semantic combinations
	DumpContextValidator commonininterfaces.DumpContextValidator

	// RestorationContextBuilder builds restoration ordering
	// and restoration dependency metadata.
	//
	// The result is later used during restore planning
	// and restoration execution.
	RestorationContextBuilder commonininterfaces.RestorationContextBuilder

	// DumpPlanAssembler combines all generated runtime artifacts
	// into a final immutable executable dump plan.
	//
	// The resulting DumpPlan contains:
	//   - dump object specs
	//   - schema dump specs
	//   - restoration context
	//   - metadata
	//   - transformation configuration
	//   - introspection snapshot
	DumpPlanAssembler commonininterfaces.DumpPlanAssembler

	// DumpPlanValidator validates the final executable dump plan.
	//
	// This stage verifies execution safety and runtime consistency.
	//
	// Examples:
	//   - restoration ordering gaps
	//   - unresolved execution dependencies
	//   - subset integrity violations
	//   - execution deadlocks
	//   - unsupported runtime combinations
	DumpPlanValidator commonininterfaces.DumpPlanValidator

	// DumpProcessor executes the final dump plan.
	//
	// This stage:
	//   - initializes dumpers
	//   - schedules execution
	//   - acquires connections
	//   - performs dumping
	//   - collects dump metadata
	//   - produces dump artifacts
	DumpProcessor commonininterfaces.DumpProcessor
}

type DumpPipeline struct {
	Stages DumpStages
	engine models.DBMSEngine
}

func (p *DumpPipeline) Run(ctx context.Context, cfg config.Config) (models.Metadata, error) {
	const (
		defaultSessionCloseTimeout = 5 * time.Second
	)

	session, err := p.Stages.DumpSessionBuilder.Open(ctx, nil)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("open dump session: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultSessionCloseTimeout)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			log.Ctx(ctx).Err(err).Msg("close dump session")
		}
	}()

	operationalDB, err := session.OperationalDB(ctx)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("open operational db: %w", err)
	}

	introspection, err := p.Stages.Introspector.Introspect(ctx, operationalDB)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("introspect: %w", err)
	}

	graph, err := p.Stages.DependencyGraphBuilder.BuildGraph(introspection)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("build dependency graph: %w", err)
	}

	previousMetadata, err := p.Stages.DumpMetadataLoader.LoadPrevious(ctx, models.PreviousMetadataLoadInput{
		Engine: p.engine,
	})
	if err != nil {
		return models.Metadata{}, fmt.Errorf("load previous metadata: %w", err)
	}

	diffResult := p.Stages.SchemaDriftValidator.Compare(models.SchemaDriftValidatorInput{
		Previous: previousMetadata.Introspection,
		Current:  introspection,
	})

	editedCfg := p.Stages.ConfigEditor.EditConfig(models.ConfigEditInput{
		Config: cfg.Dump.Transformation.ToTransformationConfig(),
	})

	subset, err := p.Stages.SubsetBuilder.BuildSubset(models.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: graph,
	})
	if err != nil {
		return models.Metadata{}, fmt.Errorf("build subset: %w", err)
	}

	explicitCtxIn := models.ExplicitDumpContextInput{
		Config:              cfg,
		TableConfigs:        editedCfg,
		IntrospectionResult: introspection,
		SubsetMapping:       subset,
	}
	explicitCtx, err := p.Stages.ExplicitDumpContextBuilder.BuildDumpContext(explicitCtxIn)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("build explicit dump context: %w", err)
	}

	finalCtx, err := p.Stages.DerivedDumpContextBuilder.BuildDumpContext(
		explicitCtxIn.ToDerivedDumpContextInput(graph),
		explicitCtx,
	)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("build derived dump context: %w", err)
	}

	// TODO: Consider weather DumpContext contains all the info required to build spashot?
	//		I see that there are no info about source. Should it be here? I don't think so.
	//		So you should consider, maybe you need to define a separate structure with enriched info
	//		from other objects.
	dumpCtxStapshot, err := p.Stages.DumpContextSnapshotBuilder.Build(ctx, finalCtx)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("build dump context snapshot: %w", err)
	}
	// TODO: You have to pass dumpCtxStapshot to DumpPlanAssembler
	// Basically DumpPlanAssembler can assamble a dump plan based on the previous artifact at all stages.

	if report, err := p.Stages.DumpContextValidator.Validate(ctx, finalCtx); err != nil {
		return models.Metadata{}, fmt.Errorf("validate dump context: %w", err)
	} else if report.HasErrors() {
		return models.Metadata{}, report.AsError()
	}

	restoration, err := p.Stages.RestorationContextBuilder.Build(ctx, models.RestorationContextInput{
		DumpContext:     finalCtx,
		DependencyGraph: graph,
	})
	if err != nil {
		return models.Metadata{}, fmt.Errorf("build restoration context: %w", err)
	}

	plan, err := p.Stages.DumpPlanAssembler.Assemble(ctx, models.DumpPlanInput{
		DumpContext:         finalCtx,
		RestorationContext:  restoration,
		IntrospectionResult: introspection,
		Config:              editedCfg,
	})
	if err != nil {
		return models.Metadata{}, fmt.Errorf("assemble dump plan: %w", err)
	}

	if report, err := p.Stages.DumpPlanValidator.Validate(ctx, plan); err != nil {
		return models.Metadata{}, fmt.Errorf("validate dump plan: %w", err)
	} else if report.HasErrors() {
		return models.Metadata{}, report.AsError()
	}

	return p.Stages.DumpProcessor.Run(ctx, plan)
}
