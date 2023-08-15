package pgcopy

import (
	"fmt"
	"strings"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

// TODO: It's not a production solution. Real copy parser must be backported.
// 	We have only two to solve it:
//		1. Implement COPY using CSV format, but I suspect it may cause escaping problems
//		2. Fully backport PostgreSQL COPY TEXT format

func LoadTuple(table *domains.TableMeta, data []byte) ([]string, error) {
	res := strings.Split(string(data), "\t")
	if len(table.Columns) != len(res) {
		return nil, fmt.Errorf("wrong tuple length: expected %d received %d", len(table.Columns), len(res))
	}
	return res, nil
}

func DumpTuple(table *domains.TableMeta, record []string) ([]byte, error) {
	if len(table.Columns) != len(record) {
		return nil, fmt.Errorf("wrong tuple length: expected %d received %d", len(table.Columns), len(record))
	}
	res := strings.Join(record, "\t")
	return []byte(res), nil
}
