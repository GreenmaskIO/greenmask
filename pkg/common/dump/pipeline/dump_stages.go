package pipeline

import (
	core "github.com/greenmaskio/greenmask/pkg/common/core"
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
	// ConnectionConfigurerBuilder translates the generic config.Config into a
	// DBMS-specific ConnectionConfigurer that carries the connection parameters
	// required by DumpSessionBuilder.
	//
	// This stage is the only place in the pipeline that knows about DBMS-specific
	// config layout (e.g. which fields come from CommonDumpOptions vs MysqlDumpConfig).
	// The pipeline itself never imports DBMS packages.
	ConnectionConfigurerBuilder core.ConnectionConfigurerBuilder

	// DumpSessionBuilder opens a DBMS-specific runtime session for the dump pipeline.
	//
	// It receives the ConnectionConfigurer produced by ConnectionConfigurerBuilder,
	// asserts it to the concrete DBMS type, and initialises all resources required
	// for a consistent dump execution:
	//
	//   - connection pools
	//   - transactional / snapshot-isolated connections
	//   - protocol-level clients
	//   - engine-specific runtime handles
	DumpSessionBuilder core.DumpSessionBuilder

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
	Introspector core.IntrospectorV2

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
	DependencyGraphBuilder core.DependencyGraphBuilder

	DumpMetadataLoader core.DumpMetadataLoader

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
	SchemaDriftValidator core.SchemaDriftValidator

	// SubsetBuilder generates subset queries and subset dependency metadata
	// using the dependency graph and configuration rules.
	//
	// This stage is responsible for planning partial data extraction
	// while preserving referential integrity.
	SubsetBuilder core.SubsetBuilder

	// ConfigEditor updates and enriches the original user configuration
	// using additional semantic information.
	//
	// Examples:
	//   - classification-driven rules
	//   - policy-generated transformations
	//   - inherited defaults
	//   - auto-generated include/exclude rules
	ConfigEditor core.ConfigEditor

	// ObjectFilter declares which introspected objects participate in the dump.
	// It sits between ConfigEditor and ExplicitDumpContextBuilder, receiving the
	// DBMS-specific dump config (from ConfigEditor) and returning the allowed
	// ObjectID set per kind.
	//
	// Note: highest-scope filtering (databases, schemas) is the Introspector's
	// responsibility. ObjectFilter handles per-object inclusion/exclusion within
	// the already-scoped introspection result.
	//
	// Downstream builders use AllowedObjects to skip excluded objects.
	// Validation stages can compare AllowedObjects against the full
	// IntrospectionResult in state.Discovery to detect anomalies.
	ObjectFilter core.ObjectFilter

	// FilterConfigBuilder withdraws the DBMS-agnostic include/exclude filtering
	// options from the user config into a core.FilterConfig, which is handed to
	// ObjectFilter alongside the introspection result.
	FilterConfigBuilder core.FilterConfigBuilder

	// ExplicitDumpContextBuilder constructs the initial dump context
	// directly from explicit user configuration and introspection results.
	//
	// This stage produces:
	//   - explicit transformers
	//   - explicit dump object specs
	//   - schema dump specs
	//   - initial runtime payloads
	ExplicitDumpContextBuilder core.ExplicitDumpContextBuilder

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
	DerivedDumpContextBuilder core.DerivedDumpContextBuilder

	// DumpContextSnapshotBuilder converts the final DumpContext into a
	// serialisable, deterministic snapshot (DumpContextSnapshot).
	//
	// The snapshot captures a stable fingerprint of the dump intent at a
	// point in time, including:
	//   - source identity and vendor parameters (snapshot ID, GTID, SCN …)
	//   - per-object subset queries and their hashes
	//   - transformation configurations and their fingerprints
	//   - object-level conditions
	//
	// The snapshot is later consumed by DumpContextDiffer to produce a
	// semantic diff against the previous dump, and is stored inside
	// Metadata so future runs can compare against it.
	DumpContextSnapshotBuilder core.DumpContextSnapshotBuilder

	// DumpContextDiffer compares dump contexts and produces
	// deterministic semantic diffs.
	//
	// This stage may be used for:
	//   - GitOps workflows
	//   - approval pipelines
	//   - UI visualization
	//   - audit history
	//   - dry-run analysis
	DumpContextDiffer core.DumpContextDiffer

	// DumpContextValidator validates semantic correctness
	// of the final dump context.
	//
	// Examples:
	//   - invalid transformer propagation
	//   - unresolved semantic conflicts
	//   - incompatible transformations
	//   - missing referenced objects
	//   - unsupported semantic combinations
	DumpContextValidator core.DumpContextValidator

	// RestorationContextBuilder builds restoration ordering
	// and restoration dependency metadata.
	//
	// The result is later used during restore planning
	// and restoration execution.
	RestorationContextBuilder core.RestorationContextBuilder

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
	DumpPlanAssembler core.DumpPlanAssembler

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
	DumpPlanValidator core.DumpPlanValidator

	// DumpProcessor executes the final dump plan.
	//
	// This stage:
	//   - initializes dumpers
	//   - schedules execution
	//   - acquires connections
	//   - performs dumping
	//   - collects dump metadata
	//   - produces dump artifacts
	DumpProcessor core.DumpProcessor
}
