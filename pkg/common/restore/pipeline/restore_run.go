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

	// Warnings holds the validation warnings collected during the run. Unlike a
	// live collector it is a plain serialisable slice, so RestoreRunState can
	// cross process boundaries (e.g. Temporal activities in gm-backend) with its
	// warnings intact. withRuntime populates it on every exit path; the
	// collector itself is an internal pipeline detail that never escapes.
	Warnings core.ValidationWarnings `json:"warnings,omitempty"`

	// Storage is the Storager scoped to the resolved dumpID. Set by Execute
	// after RestoreStorageProvisioner.Provision succeeds.
	Storage core.Storager

	// Metadata holds the dump metadata read from Storage. Set by Execute after
	// RestoreMetadataReader.ReadMetadata succeeds.
	Metadata *core.Metadata

	// TargetIntrospection holds runtime facts about the target database (e.g.
	// its server version) read by the RestoreIntrospector before execution.
	TargetIntrospection *core.RestoreIntrospectionResult

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

// HasWarnings reports whether any validation warning was collected during the
// run. Mirrors Collector.HasWarnings against the serialised slice.
func (r *RestoreRunState) HasWarnings() bool {
	return r.Warnings.HasWarnings()
}

// IsFatal reports whether any collected warning has error severity. Mirrors
// Collector.IsFatal against the serialised slice.
func (r *RestoreRunState) IsFatal() bool {
	return r.Warnings.IsFatal()
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
