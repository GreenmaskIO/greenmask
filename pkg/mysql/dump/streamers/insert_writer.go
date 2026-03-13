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

const DefaultInsertBatchSize = 100

var (
	// rowSeparatorBytes and insertTerminatorBytes are preallocated to avoid
	// string allocations during Write and Flush, reducing GC overhead.
	rowSeparatorBytes     = []byte(",\n")
	insertTerminatorBytes = []byte(";\n")
)

type InsertWriter struct {
	table         *models.Table
	w             io.Writer
	vals          []interface{}
	rowTemplate   string
	headerWritten bool
	batchSize     int
	rowsCount     int
	header        []byte
}

func NewInsertWriter(table models.Table, w io.Writer, batchSize int) *InsertWriter {
	placeholders := make([]string, len(table.Columns))
	for i := range table.Columns {
		placeholders[i] = "?"
	}
	rowTemplate := "(" + strings.Join(placeholders, ", ") + ")"

	tableName := sqlbuilder.MySQL.Quote(table.Name)
	if table.Schema != "" {
		tableName = fmt.Sprintf("%s.%s", sqlbuilder.MySQL.Quote(table.Schema), tableName)
	}
	headerStr := fmt.Sprintf("INSERT INTO %s (", tableName)
	for i, col := range table.Columns {
		if i > 0 {
			headerStr += ", "
		}
		headerStr += sqlbuilder.MySQL.Quote(col.Name)
	}
	headerStr += ") VALUES \n"

	return &InsertWriter{
		table:       &table,
		w:           w,
		vals:        make([]interface{}, len(table.Columns)),
		rowTemplate: rowTemplate,
		batchSize:   batchSize,
		header:      []byte(headerStr),
	}
}

func (iw *InsertWriter) writeHeader() error {
	if _, err := iw.w.Write(iw.header); err != nil {
		return err
	}
	return nil
}

func (iw *InsertWriter) Write(row [][]byte) error {
	if !iw.headerWritten {
		if err := iw.writeHeader(); err != nil {
			return fmt.Errorf("write header: %w", err)
		}
		iw.headerWritten = true
	} else {
		if _, err := iw.w.Write(rowSeparatorBytes); err != nil {
			return fmt.Errorf("write row separator: %w", err)
		}
	}

	for i, val := range row {
		if val == nil || bytes.Equal(val, dbmsdriver.NullValueSeq) {
			iw.vals[i] = nil
		} else {
			iw.vals[i] = string(val)
		}
	}

	interpolated, err := sqlbuilder.MySQL.Interpolate(iw.rowTemplate, iw.vals)
	if err != nil {
		return fmt.Errorf("interpolate row: %w", err)
	}

	if _, err := fmt.Fprint(iw.w, interpolated); err != nil {
		return fmt.Errorf("write row: %w", err)
	}

	iw.rowsCount++

	if iw.batchSize == 0 || iw.rowsCount == iw.batchSize {
		if _, err := iw.w.Write(insertTerminatorBytes); err != nil {
			return fmt.Errorf("terminate insert: %w", err)
		}
		iw.headerWritten = false
		iw.rowsCount = 0
	}

	return nil
}

func (iw *InsertWriter) Flush() error {
	if iw.headerWritten {
		if _, err := iw.w.Write(insertTerminatorBytes); err != nil {
			return fmt.Errorf("terminate insert: %w", err)
		}
		iw.headerWritten = false
		iw.rowsCount = 0
	}
	return nil
}
