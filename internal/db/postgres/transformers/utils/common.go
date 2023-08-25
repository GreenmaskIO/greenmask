package utils

import (
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/data_section"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgcopy"
)

const (
	DefaultNullSeq      = `\N`
	DefaultNullFraction = 0.3
)

func GetColumnValueFromCsvRecord(table *data_section.Table, data []byte, columnNum int) ([]string, string, error) {
	record, err := ParseCsvRecord(table, data)
	if err != nil {
		return nil, "", err
	}
	return record, record[columnNum], nil
}

func UpdateAttributeAndBuildRecord(table *data_section.Table, data []string, val string, columnNum int) ([]byte, error) {
	data[columnNum] = val
	return BuildCsvRecord(table, data)
}

func ParseCsvRecord(table *data_section.Table, data []byte) ([]string, error) {
	return pgcopy.LoadTuple(table, data)
}

func BuildCsvRecord(table *data_section.Table, data []string) ([]byte, error) {
	return pgcopy.DumpTuple(table, data)
}
