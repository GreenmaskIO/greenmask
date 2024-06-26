package entries

import "github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"

type CycleResolutionOp struct {
	Columns  []string
	FileName string
	Row      *pgcopy.Row
}

func NewCycleResolutionOp(fileName string, columns []string) *CycleResolutionOp {
	return &CycleResolutionOp{
		FileName: fileName,
		Columns:  columns,
		Row:      pgcopy.NewRow(len(columns)),
	}
}
