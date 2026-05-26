package pipeline

import (
	"fmt"
	"strings"

	"github.com/greenmaskio/greenmask/pkg/common/models"
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

type DiscoveryStageArtifacts struct {
	Config           *config.Config                `json:"config"`
	Introspection    *models.IntrospectionResult   `json:"introspection"`
	DependencyGraph  *models.DependencyGraphResult `json:"dependency_graph"`
	PreviousMetadata *models.Metadata              `json:"previous_metadata,omitempty"`
	SchemaDrift      *models.SchemaDriftResult     `json:"schema_drift,omitempty"`
	Subset           *models.SubsetResult          `json:"subset,omitempty"`
}

type ContextStageArtifacts struct {
	EditedConfig any                 `json:"edited_config,omitempty"`
	ExplicitCtx  *models.DumpContext `json:"explicit_ctx,omitempty"`
	FinalCtx     *models.DumpContext `json:"final_ctx,omitempty"`
}

type BuildSnapshotAndDiffArtifacts struct {
	DumpContextSnapshot *models.DumpContextSnapshot `json:"dump_context_snapshot,omitempty"`
	DumpContextDiff     *models.DumpContextDiff     `json:"dump_context_diff,omitempty"`
}

type BuildPlanArtifacts struct {
	Plan *models.DumpPlan `json:"plan,omitempty"`
}

//type RuntimeRunArtifacts struct {
//	Runtime *Runtime `json:"-"`
//
//	ExplicitDumpContext *models.DumpContext `json:"-"`
//	FinalDumpContext    *models.DumpContext `json:"-"`
//
//	DumpPlan *models.DumpPlan `json:"-"`
//}

type ExecuteStageArtifacts struct {
	Metadata *models.Metadata `json:"metadata,omitempty"`
}

type RunState struct {
	ExecutedStages      map[StageName]bool
	ExecutedStagesOrder []StageName

	Discovery            DiscoveryStageArtifacts       `json:"discovery"`
	Context              ContextStageArtifacts         `json:"context"`
	BuildSnapshotAndDiff BuildSnapshotAndDiffArtifacts `json:"build_snapshot_and_diff"`
	BuildPlan            BuildPlanArtifacts            `json:"build_plan_artifacts"`
	ExecuteStage         ExecuteStageArtifacts         `json:"execute_stage_artifacts"`
}

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
		ExecutedStages: executedStages,
	}
}

func (r *RunState) MarkExecuted(stage StageName) {
	r.ExecutedStagesOrder = append(r.ExecutedStagesOrder, stage)
	r.ExecutedStages[stage] = true
}

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
