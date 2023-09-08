package utils

import (
	"github.com/greenmaskio/greenmask/internal/db/postgres/lib/pgcopy"

	"github.com/greenmaskio/greenmask/internal/db/postgres/domains/toclib"
)

const (
	DefaultNullSeq      = `\N`
	DefaultNullFraction = 0.3
)

func GetColumnValueFromCsvRecord(table *toclib.Table, data []byte, columnNum int) ([]string, string, error) {
	record, err := ParseCsvRecord(table, data)
	if err != nil {
		return nil, "", err
	}
	return record, record[columnNum], nil
}

func UpdateAttributeAndBuildRecord(table *toclib.Table, data []string, val string, columnNum int) ([]byte, error) {
	data[columnNum] = val
	return BuildCsvRecord(table, data)
}

func ParseCsvRecord(table *toclib.Table, data []byte) ([]string, error) {
	return pgcopy.LoadTuple(table, data)
}

func BuildCsvRecord(table *toclib.Table, data []string) ([]byte, error) {
	return pgcopy.DumpTuple(table, data)
}
