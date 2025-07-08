package models

import (
	"errors"
	"fmt"
)

var (
	ErrFatalValidationError           = errors.New("fatal validation error")
	ErrCanonicalTypeMismatch          = errors.New("canonical type mismatch")
	ErrUnknownColumnName              = errors.New("unknown column name")
	ErrCheckTransformerImplementation = errors.New("check transformer implementation")
)

type DumpError struct {
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
	Line   uint64 `json:"line,omitempty"`
	Err    error  `json:"err,omitempty"`
}

func NewDumpError(schema, table string, line uint64, err error) *DumpError {
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
