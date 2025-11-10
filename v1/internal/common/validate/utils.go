package validate

import (
	"slices"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/mitchellh/go-wordwrap"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

var endOfFileSeq = []byte(`\.`)

const nullStringValue = "NULL"

func getAffectedColumns(t *entries.Table) map[string]struct{} {
	affectedColumns := make(map[string]struct{})
	for _, tr := range t.TransformersContext {
		ac := tr.Transformer.GetAffectedColumns()
		for _, name := range ac {
			affectedColumns[name] = struct{}{}
		}
	}
	return affectedColumns
}

func LineIsEndOfData(line []byte) bool {
	return len(endOfFileSeq) == len(line) && line[0] == '\\' && line[1] == '.'
}

func ValuesEqual(a, b *commonmodels.ColumnRawValue) bool {
	return a.IsNull == b.IsNull && slices.Equal(a.Data, b.Data)
}

func getStringFromRawValue(v *commonmodels.ColumnRawValue) string {
	if v.IsNull {
		return nullStringValue
	}
	return string(v.Data)
}

func WrapString(v string, maxLength int) string {
	strs := strings.Split(wordwrap.WrapString(v, uint(maxLength)), "\n")
	res := make([]string, 0, len(strs))
	for _, s := range strs {
		if len(s) > maxLength {

			for idx := 0; idx < len(s); idx += maxLength {
				rest := idx + maxLength
				if rest > len(s) {
					rest = idx + len(s) - idx
				}
				res = append(res, s[idx:rest])
			}

		} else {
			res = append(res, s)
		}
	}
	return strings.Join(res, "\n")
}
