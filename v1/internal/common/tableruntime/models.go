package tableruntime

import (
	"github.com/greenmaskio/greenmask/v1/internal/common/conditions"
	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

// TransformerRuntime - supplied transformer and conditions that have to be executed.
type TransformerRuntime struct {
	Transformer commonininterfaces.Transformer
	WhenCond    *conditions.WhenCond
}

// TableRuntime - everything related to the table that must be applied for a record.
// It contains table, transformers, dump query, table driver and conditions.
type TableRuntime struct {
	Table               *commonmodels.Table
	TransformerRuntimes []*TransformerRuntime
	TableCondition      *conditions.WhenCond
	Query               string
	Driver              commonininterfaces.TableDriver
}
