package pgcopy

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

// TODO: It's not a production solution. Real copy parser must be backported.
// 	We have only two to solve it:
//		1. Implement COPY using CSV format, but I suspect it may cause escaping problems
//		2. Fully backport PostgreSQL COPY TEXT format

func LoadTuple2(table *domains.TableMeta, data []byte) ([]string, error) {
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

func DumpTuple2(table *domains.TableMeta, record []string) ([]byte, error) {
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

func LoadTuple(table *domains.TableMeta, data []byte) ([]string, error) {
	res := strings.Split(string(data[:len(data)-1]), "\t")
	if len(table.Columns) != len(res) {
		return nil, fmt.Errorf("wrong tuple length: expected %d received %d", len(table.Columns), len(res))
	}
	return res, nil
}

func DumpTuple(table *domains.TableMeta, record []string) ([]byte, error) {
	if len(table.Columns) != len(record) {
		return nil, fmt.Errorf("wrong tuple length: expected %d received %d", len(table.Columns), len(record))
	}
	res := []byte(strings.Join(record, "\t"))
	res = append(res, '\n')
	return res, nil
}
