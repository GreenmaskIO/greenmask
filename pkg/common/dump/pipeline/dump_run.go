package pipeline

import (
	"fmt"
	"strings"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
)

type StageName string

const (
	StageNameSessionInitialization StageName = "session_initialization"
	StageNameDiscovery             StageName = "discovery"
	StageNameContextBuilding       StageName = "context_building"
	StageNameSnapshotDiffBuilding  StageName = "snapshot_diff_building"
	StageNameContextValidation     StageName = "context_validation"
	StageNamePlanBuilding          StageName = "plan_building"
	StageNamePlanValidation        StageName = "plan_validation"
	StageNameExecution             StageName = "execution"
)

// DiscoveryStageArtifacts holds everything produced by the Discover stage:
// a live DB introspection, the dependency graph, previous-run metadata (nil on
// first run), schema drift compared to that metadata, and the resolved subset.
type DiscoveryStageArtifacts struct {
	Config           *config.Config              `json:"config"`
	FilterConfig     *core.FilterConfig          `json:"filter_config,omitempty"`
	Introspection    *core.IntrospectionResult   `json:"introspection"`
	DependencyGraph  *core.DependencyGraphResult `json:"dependency_graph"`
	PreviousMetadata *core.Metadata              `json:"previous_metadata,omitempty"`
	SchemaDrift      *core.SchemaDriftResult     `json:"schema_drift,omitempty"`
	Subset           *core.SubsetResult          `json:"subset,omitempty"`
}

// ContextStageArtifacts holds the outputs of BuildContext: the config after
// any drift-driven edits, the explicitly configured DumpContext, and the final
// derived DumpContext that all subsequent stages consume.
type ContextStageArtifacts struct {
	EditedConfig       any                     `json:"edited_config,omitempty"`
	ObjectFilterResult core.ObjectFilterResult `json:"object_filter_result,omitempty"`
	ExplicitCtx        *core.DumpContext       `json:"explicit_ctx,omitempty"`
	FinalCtx           *core.DumpContext       `json:"final_ctx,omitempty"`
}

// BuildSnapshotAndDiffArtifacts holds the serialisable snapshot of the current
// DumpContext and the diff against the previous run's snapshot. Both are
// required by ValidateContext and BuildPlan.
type BuildSnapshotAndDiffArtifacts struct {
	DumpContextSnapshot *core.DumpContextSnapshot `json:"dump_context_snapshot,omitempty"`
	DumpContextDiff     *core.DumpContextDiff     `json:"dump_context_diff,omitempty"`
}

// BuildPlanArtifacts holds the assembled DumpPlan produced by BuildPlan.
// It is the sole input to Execute.
type BuildPlanArtifacts struct {
	Plan *core.DumpPlan `json:"plan,omitempty"`
}

//type RuntimeRunArtifacts struct {
//	Runtime *Runtime `json:"-"`
//
//	ExplicitDumpContext *core.DumpContext `json:"-"`
//	FinalDumpContext    *core.DumpContext `json:"-"`
//
//	DumpPlan *core.DumpPlan `json:"-"`
//}

// ExecuteStageArtifacts holds the Metadata written to storage after a
// successful Execute call.
type ExecuteStageArtifacts struct {
	Metadata *core.Metadata `json:"metadata,omitempty"`
}

// RunState is the shared mutable ledger that flows through an entire dump
// operation. Each stage appends its outputs to the relevant artifact field and
// records itself via MarkExecuted; subsequent stages call Require to gate on
// their prerequisites.
//
// All artifact fields carry JSON tags so RunState can be serialised between
// process boundaries — for example, across Temporal workflow activities in
// gm-backend. RunState deliberately holds no live resources (connections,
// file handles); those belong to Runtime, which is passed as a separate
// parameter to the stages that need them.
type RunState struct {
	DumpID              core.DumpID `json:"dump_id"`
	ExecutedStages      map[StageName]bool
	ExecutedStagesOrder []StageName

	// Warnings holds the validation warnings collected during the run. Unlike a
	// live collector it is a plain serialisable slice, so RunState can cross
	// process boundaries (e.g. Temporal activities in gm-backend) with its
	// warnings intact. withRuntime populates it on every exit path; the
	// collector itself is an internal pipeline detail that never escapes.
	Warnings core.ValidationWarnings `json:"warnings,omitempty"`

	Discovery            DiscoveryStageArtifacts       `json:"discovery"`
	Context              ContextStageArtifacts         `json:"context"`
	BuildSnapshotAndDiff BuildSnapshotAndDiffArtifacts `json:"build_snapshot_and_diff"`
	BuildPlan            BuildPlanArtifacts            `json:"build_plan_artifacts"`
	ExecuteStage         ExecuteStageArtifacts         `json:"execute_stage_artifacts"`
}

// NewRunState creates a RunState with all stages marked as not yet executed
// and the supplied config stored in Discovery so every subsequent stage can
// access it without an extra parameter. A unique DumpID is generated at
// construction time so it is available (and serialisable) before Execute runs.
func NewRunState(cfg config.Config) *RunState {
	executedStages := map[StageName]bool{
		StageNameSessionInitialization: false,
		StageNameDiscovery:             false,
		StageNameContextBuilding:       false,
		StageNameSnapshotDiffBuilding:  false,
		StageNameContextValidation:     false,
		StageNamePlanBuilding:          false,
		StageNamePlanValidation:        false,
		StageNameExecution:             false,
	}
	return &RunState{
		DumpID:         core.NewDumpID(),
		ExecutedStages: executedStages,
		Discovery: DiscoveryStageArtifacts{
			Config: &cfg,
		},
	}
}

// HasWarnings reports whether any validation warning was collected during the
// run. Mirrors Collector.HasWarnings against the serialised slice.
func (r *RunState) HasWarnings() bool {
	return r.Warnings.HasWarnings()
}

// IsFatal reports whether any collected warning has error severity. Mirrors
// Collector.IsFatal against the serialised slice.
func (r *RunState) IsFatal() bool {
	return r.Warnings.IsFatal()
}

// MarkExecuted records stage as completed, both in the map (for O(1) Require
// checks) and in the ordered slice (for diagnostics / audit trails).
func (r *RunState) MarkExecuted(stage StageName) {
	r.ExecutedStagesOrder = append(r.ExecutedStagesOrder, stage)
	r.ExecutedStages[stage] = true
}

// Require returns an error listing every stage in stages that has not yet been
// executed. Call it at the top of each pipeline stage to enforce ordering.
func (r *RunState) Require(stages ...StageName) error {
	var missing []string
	for _, stage := range stages {
		if !r.ExecutedStages[stage] {
			missing = append(missing, string(stage))
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("required stages not executed: [%s]", strings.Join(missing, ", "))
}
