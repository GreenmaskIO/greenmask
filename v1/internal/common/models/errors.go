package models

import (
	"errors"
	"fmt"
)

var (
	ErrFatalValidationError                        = errors.New("fatal validation error")
	ErrCanonicalTypeMismatch                       = errors.New("canonical type mismatch")
	ErrUnknownColumnName                           = errors.New("unknown column name")
	ErrCheckTransformerImplementation              = errors.New("check transformer implementation")
	ErrProvidedRowLengthIsNotEqualToTheDestination = errors.New("provided row length is not equal to destination")
	ErrUnknownColumnIdx                            = errors.New("unknown column index")
	ErrEndOfStream                                 = errors.New("end of stream")
)

type DumpError struct {
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
	Line   int64  `json:"line,omitempty"`
	Err    error  `json:"err,omitempty"`
}

func NewDumpError(line int64, err error) *DumpError {
	return &DumpError{
		Line: line,
		Err:  err,
	}
}

func (de *DumpError) Error() string {
	return fmt.Sprintf("at line %d: %s", de.Line, de.Err.Error())
}
