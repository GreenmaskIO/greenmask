package dumpers

import "fmt"

type DumpError struct {
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
	Line   int64  `json:"line,omitempty"`
	Err    error  `json:"err,omitempty"`
}

func NewDumpError(schema, table string, line int64, err error) *DumpError {
	return &DumpError{
		Schema: schema,
		Table:  table,
		Line:   line,
		Err:    err,
	}
}

func (de *DumpError) Error() string {
	return fmt.Sprintf("dump error on table %s.%s at line %d: %s", de.Schema, de.Table, de.Line, de.Err.Error())
}
