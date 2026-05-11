// Copyright 2023 Greenmask
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

package dumpers

import "fmt"

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
