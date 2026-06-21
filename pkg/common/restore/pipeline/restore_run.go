package pipeline

import (
	"fmt"
	"strings"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
)

type RestoreStageName string

const (
	RestoreStageNameSessionInitialization RestoreStageName = "session_initialization"
	RestoreStageNameExecution             RestoreStageName = "execution"
)

// RestoreExecuteArtifacts holds the outputs of the Execute phase.
// Currently empty but present for symmetry with DumpStages artifacts and
// future extensibility (e.g. restore statistics).
type RestoreExecuteArtifacts struct{}

// RestoreRunState is the mutable ledger that flows through a restore operation.
//
// It holds no live resources — those belong to RestoreRuntime. All fields are
// serialisable so RunState can cross process boundaries (e.g. Temporal
// workflow activities in gm-backend).
type RestoreRunState struct {
	ExecutedStages      map[RestoreStageName]bool
	ExecutedStagesOrder []RestoreStageName

	Config config.Config
	DumpID core.DumpID

	// Storage is the Storager scoped to the resolved dumpID. Set by Execute
	// after RestoreStorageProvisioner.Provision succeeds.
	Storage core.Storager

	// Metadata holds the dump metadata read from Storage. Set by Execute after
	// RestoreMetadataReader.ReadMetadata succeeds.
	Metadata *core.Metadata

	ExecuteStage RestoreExecuteArtifacts
}

// NewRestoreRunState creates a RestoreRunState with all stages marked not yet
// executed.
func NewRestoreRunState(cfg config.Config, dumpID core.DumpID) *RestoreRunState {
	return &RestoreRunState{
		Config: cfg,
		DumpID: dumpID,
		ExecutedStages: map[RestoreStageName]bool{
			RestoreStageNameSessionInitialization: false,
			RestoreStageNameExecution:             false,
		},
	}
}

// MarkExecuted records stage as completed in both the map (O(1) Require checks)
// and the ordered slice (diagnostics / audit trail).
func (r *RestoreRunState) MarkExecuted(stage RestoreStageName) {
	r.ExecutedStagesOrder = append(r.ExecutedStagesOrder, stage)
	r.ExecutedStages[stage] = true
}

// Require returns an error listing every stage in stages that has not yet
// been executed. Call at the top of each pipeline phase to enforce ordering.
func (r *RestoreRunState) Require(stages ...RestoreStageName) error {
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
