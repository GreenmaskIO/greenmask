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

package streamers

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/huandu/go-sqlbuilder"

	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
)

// InsertWriter writes one value tuple per line:
//
//	(val1,val2,val3)
//	(val4,val5,val6)
//
// No INSERT keyword, table name, commas between rows, or semicolons are written.
// The INSERT statement is assembled on the restore side, allowing the conflict
// resolution strategy (INSERT / INSERT IGNORE / REPLACE) and batch size to be
// chosen at restore time.
type InsertWriter struct {
	table       *models.Table
	w           io.Writer
	vals        []interface{}
	rowTemplate string
}

func NewInsertWriter(table models.Table, w io.Writer) *InsertWriter {
	placeholders := make([]string, len(table.Columns))
	for i := range table.Columns {
		placeholders[i] = "?"
	}
	rowTemplate := "(" + strings.Join(placeholders, ", ") + ")"
	return &InsertWriter{
		table:       &table,
		w:           w,
		vals:        make([]interface{}, len(table.Columns)),
		rowTemplate: rowTemplate,
	}
}

func (iw *InsertWriter) Write(row [][]byte) error {
	for i, val := range row {
		if bytes.Equal(val, dbmsdriver.NullValueSeq) {
			iw.vals[i] = nil
		} else {
			iw.vals[i] = string(val)
		}
	}

	interpolated, err := sqlbuilder.MySQL.Interpolate(iw.rowTemplate, iw.vals)
	if err != nil {
		return fmt.Errorf("interpolate row: %w", err)
	}

	if _, err := fmt.Fprintf(iw.w, "%s\n", interpolated); err != nil {
		return fmt.Errorf("write row: %w", err)
	}
	return nil
}

// Flush is a no-op: each row is written atomically with its newline.
func (iw *InsertWriter) Flush() error {
	return nil
}
