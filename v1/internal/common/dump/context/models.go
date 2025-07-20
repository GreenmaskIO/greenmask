package context

import (
	"github.com/greenmaskio/greenmask/v1/internal/common/conditions"
	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

// TransformerContext - supplied transformer and conditions that have to be executed.
type TransformerContext struct {
	Transformer commonininterfaces.Transformer
	WhenCond    *conditions.WhenCond
}

func (tc *TransformerContext) EvaluateWhen(r commonininterfaces.Recorder) (bool, error) {
	if tc.WhenCond == nil {
		return true, nil
	}
	return tc.WhenCond.Evaluate(r)
}

// TableContext - everything related to the table that must be applied for a record.
// It contains table, transformers, dump query, table driver and conditions.
type TableContext struct {
	Table              *commonmodels.Table
	TransformerContext []*TransformerContext
	TableCondition     *conditions.WhenCond
	Query              string
	TableDriver        commonininterfaces.TableDriver
}

func (tc *TableContext) HasTransformer() bool {
	return len(tc.TransformerContext) > 0
}
