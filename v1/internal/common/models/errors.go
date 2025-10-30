// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	ErrTableGraphHasCycles                         = errors.New("table graph has cycles")
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
