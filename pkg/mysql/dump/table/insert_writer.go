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

package table

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
)

const hexChars = "0123456789ABCDEF"

// InsertWriter writes one value tuple per line:
//
//	(val1,val2,val3)
//	(val4,val5,val6)
//
// No INSERT keyword, table name, commas between rows, or semicolons are written.
// The INSERT statement is assembled on the restore side, allowing the conflict
// resolution strategy (INSERT / INSERT IGNORE / REPLACE) and batch size to be
// chosen at restore time.
//
// When hexBlob is true, columns of binary/blob types are emitted as X'...' hex
// literals instead of escaped string literals, making the dump charset-independent
// and safe for arbitrary byte values.
type InsertWriter struct {
	table      *core.Table
	w          io.Writer
	isBinary   []bool // pre-computed: true for BINARY/VARBINARY/BLOB family columns
	hasHexCols bool   // true when any column in isBinary is set
	sb         strings.Builder
	scratch    []byte // reusable buffer for the MySQL string escaper, kept across rows
}

func NewInsertWriter(table core.Table, w io.Writer, hexBlob bool) *InsertWriter {
	isBinary := make([]bool, len(table.Columns))
	hasHexCols := false
	for i, col := range table.Columns {
		if hexBlob && isBinaryType(col) {
			isBinary[i] = true
			hasHexCols = true
		}
	}
	return &InsertWriter{
		table:      &table,
		w:          w,
		isBinary:   isBinary,
		hasHexCols: hasHexCols,
	}
}

// isBinaryType reports whether the column belongs to the binary/blob type class.
// TypeClass is used rather than TypeName because MySQL reports COLUMN_TYPE as
// "binary(16)" or "varbinary(255)" (with length suffix), which does not match
// the bare type-name constants. TypeClass is always resolved correctly via the
// DATA_TYPE fallback in the introspector.
func isBinaryType(col core.Column) bool {
	return col.Type.Class == core.TypeClassBinary
}

func (iw *InsertWriter) Write(row [][]byte) error {
	iw.sb.Reset()
	iw.sb.WriteByte('(')
	for i, val := range row {
		if i > 0 {
			iw.sb.WriteString(", ")
		}
		switch {
		case bytes.Equal(val, core.NullValueSeq):
			iw.sb.WriteString("NULL")
		case iw.hasHexCols && iw.isBinary[i]:
			iw.sb.WriteString("X'")
			for _, b := range val {
				iw.sb.WriteByte(hexChars[b>>4])
				iw.sb.WriteByte(hexChars[b&0x0f])
			}
			iw.sb.WriteByte('\'')
		default:
			// Previous implementation (kept for reference; see AppendMySQLQuotedString and
			// the differential test/benchmark in pkg/mysql/dbmsdriver):
			//   s, err := sqlbuilder.MySQL.Interpolate("?", []interface{}{string(val)})
			//   if err != nil {
			//       return fmt.Errorf("interpolate col %d: %w", i, err)
			//   }
			//   iw.sb.WriteString(s)
			iw.scratch = dbmsdriver.AppendMySQLQuotedString(iw.scratch[:0], val)
			iw.sb.Write(iw.scratch)
		}
	}
	iw.sb.WriteString(")\n")
	if _, err := fmt.Fprint(iw.w, iw.sb.String()); err != nil {
		return fmt.Errorf("write row: %w", err)
	}
	return nil
}

// Flush is a no-op: each row is written atomically with its newline.
func (iw *InsertWriter) Flush() error {
	return nil
}
