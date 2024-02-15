package validate_utils

import (
	"slices"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump_objects"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var endOfFileSeq = []byte(`\.`)

const nullStringValue = "NULL"

func getAffectedColumns(t *dump_objects.Table) map[string]struct{} {
	affectedColumns := make(map[string]struct{})
	for _, tr := range t.Transformers {
		ac := tr.GetAffectedColumns()
		for _, name := range ac {
			affectedColumns[name] = struct{}{}
		}
	}
	return affectedColumns
}

func LineIsEndOfData(line []byte) bool {
	return len(endOfFileSeq) == len(line) && line[0] == '\\' && line[1] == '.'
}

func ValuesEqual(a *toolkit.RawValue, b *toolkit.RawValue) bool {
	return a.IsNull == b.IsNull && slices.Equal(a.Data, b.Data)
}

func getPrimaryKeyConstraintColumns(t *dump_objects.Table) map[int]*toolkit.Column {
	idx := slices.IndexFunc(t.Constraints, func(constraint toolkit.Constraint) bool {
		return constraint.Type() == toolkit.PkConstraintType
	})
	if idx == -1 {
		return nil
	}
	pk := t.Constraints[idx].(*toolkit.PrimaryKey)

	columns := make(map[int]*toolkit.Column, len(pk.Columns))

	for _, attNum := range pk.Columns {
		columnIdx := slices.IndexFunc(t.Columns, func(column *toolkit.Column) bool {
			return column.Num == attNum
		})
		if columnIdx == -1 {
			panic("unable to find column by attnum")
		}
		columns[columnIdx] = t.Columns[columnIdx]
	}
	return columns
}

func getStringFromRawValue(v *toolkit.RawValue) string {
	if v.IsNull {
		return nullStringValue
	}
	return string(v.Data)
}
