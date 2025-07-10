package tablestreamer

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type Table struct{}

func NewTable() *Table {
	return &Table{}
}

func (t Table) Dump(ctx context.Context) (commonmodels.DumpStat, error) {
	//TODO implement me
	panic("implement me")
}

func (t Table) DebugInfo() string {
	//TODO implement me
	panic("implement me")
}
