package pgcopy

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/data_section"
)

// TODO: It's not a production solution. Real copy parser must be backported.
// 	We have only two to solve it:
//		1. Implement COPY using CSV format, but I suspect it may cause escaping problems
//		2. Fully backport PostgreSQL COPY TEXT format

func LoadTuple(table *data_section.Table, data []byte) ([]string, error) {
	lineReader := csv.NewReader(bytes.NewReader(data))
	values, err := lineReader.Read()
	if err != nil {
		return nil, fmt.Errorf("cannot read dump line: %w", err)
	}
	if len(table.Columns) != len(values) {
		return nil, fmt.Errorf("wrong tuple length: expected %d received %d", len(table.Columns), len(values))
	}
	return values, nil
}

func DumpTuple(table *data_section.Table, record []string) ([]byte, error) {
	if len(table.Columns) != len(record) {
		return nil, fmt.Errorf("wrong tuple length: expected %d received %d", len(table.Columns), len(record))
	}
	buf := bytes.Buffer{}
	lineWriter := csv.NewWriter(&buf)
	if err := lineWriter.Write(record); err != nil {
		return nil, fmt.Errorf("unnable to write line: %w", err)
	}
	lineWriter.Flush()

	res, err := io.ReadAll(&buf)
	if err != nil {
		return nil, fmt.Errorf("cannot read data from copy reader: %w", err)
	}
	return res, nil
}
