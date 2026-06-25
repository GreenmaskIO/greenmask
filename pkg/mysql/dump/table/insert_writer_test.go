package table

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/rawrecord"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
)

func TestInsertWriter_Write(t *testing.T) {

	t.Run("single_row", func(t *testing.T) {
		table := core.Table{
			Schema: "public",
			Name:   "users",
			Columns: []core.Column{
				{Name: "id"},
				{Name: "name"},
				{Name: "email"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, false)

		row := [][]byte{[]byte("1"), []byte("John Doe"), []byte("john@example.com")}
		err := iw.Write(row)
		assert.NoError(t, err)

		expected := "('1', 'John Doe', 'john@example.com')\n"
		assert.Equal(t, expected, buf.String())
	})

	t.Run("multiple_rows", func(t *testing.T) {
		table := core.Table{
			Schema: "public",
			Name:   "users",
			Columns: []core.Column{
				{Name: "id"},
				{Name: "name"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, false)

		err := iw.Write([][]byte{[]byte("1"), []byte("John Doe")})
		assert.NoError(t, err)
		err = iw.Write([][]byte{[]byte("2"), []byte("Jane Doe")})
		assert.NoError(t, err)
		err = iw.Flush()
		assert.NoError(t, err)

		expected := "('1', 'John Doe')\n('2', 'Jane Doe')\n"
		assert.Equal(t, expected, buf.String())
	})

	t.Run("null_values", func(t *testing.T) {
		table := core.Table{
			Name: "test",
			Columns: []core.Column{
				{Name: "id"},
				{Name: "val"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, false)

		row := [][]byte{[]byte("1"), core.NullValueSeq}
		err := iw.Write(row)
		assert.NoError(t, err)

		expected := "('1', NULL)\n"
		assert.Equal(t, expected, buf.String())
	})

	t.Run("literal_null_sequence", func(t *testing.T) {
		table := core.Table{
			Name: "test",
			Columns: []core.Column{
				{Name: "id"},
				{Name: "val"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, false)

		rr := rawrecord.NewRawRecord(2, core.NullValueSeq)
		err := rr.SetColumn(0, core.NewColumnRawValue([]byte("1"), false))
		assert.NoError(t, err)
		err = rr.SetColumn(1, core.NewColumnRawValue(core.NullValueSeq, false))
		assert.NoError(t, err)

		err = iw.Write(rr.GetRow())
		assert.NoError(t, err)

		// RawRecord escapes "\N" (1 backslash) to "\\N" (2 backslashes).
		// go-sqlbuilder for MySQL escapes each backslash to two.
		// So "\\N" becomes "\\\\N" in the SQL.
		expected := "('1', '\\\\\\\\N')\n"
		assert.Equal(t, expected, buf.String())
	})

	t.Run("flush_is_noop", func(t *testing.T) {
		table := core.Table{
			Name:    "empty_table",
			Columns: []core.Column{{Name: "id"}},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, false)

		err := iw.Flush()
		assert.NoError(t, err)
		assert.Empty(t, buf.String())
	})
}

func TestInsertWriter_HexBlob(t *testing.T) {
	// binaryCol mirrors what the introspector produces: TypeName may include a length
	// suffix (e.g. "binary(16)"), so TypeClass is the reliable signal.
	binaryCol := func(typeName string) core.Column {
		return core.Column{Name: "data", TypeName: typeName, TypeClass: core.TypeClassBinary}
	}

	binaryTypes := []string{
		"binary(16)", // COLUMN_TYPE form with length suffix
		"varbinary(255)",
		dbmsdriver.TypeTinyBlob,
		dbmsdriver.TypeBlob,
		dbmsdriver.TypeMediumBlob,
		dbmsdriver.TypeLongBlob,
	}

	t.Run("all_binary_types_produce_hex_literal", func(t *testing.T) {
		for _, typeName := range binaryTypes {
			var buf bytes.Buffer
			table := core.Table{
				Name:    "t",
				Columns: []core.Column{binaryCol(typeName)},
			}
			iw := NewInsertWriter(table, &buf, true)
			err := iw.Write([][]byte{{0xDE, 0xAD, 0xBE, 0xEF}})
			assert.NoError(t, err, "type %s", typeName)
			assert.Equal(t, "(X'DEADBEEF')\n", buf.String(), "type %s", typeName)
		}
	})

	t.Run("high_bytes_invalid_utf8", func(t *testing.T) {
		table := core.Table{
			Name:    "t",
			Columns: []core.Column{binaryCol(dbmsdriver.TypeBlob)},
		}
		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, true)
		// Bytes > 0x7F that are invalid utf8mb4 sequences — unsafe without hex-blob.
		err := iw.Write([][]byte{{0x80, 0x81, 0xFE, 0xFF}})
		assert.NoError(t, err)
		assert.Equal(t, "(X'8081FEFF')\n", buf.String())
	})

	t.Run("null_byte_and_escape_chars", func(t *testing.T) {
		table := core.Table{
			Name:    "t",
			Columns: []core.Column{binaryCol(dbmsdriver.TypeVarBinary)},
		}
		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, true)
		// \x00 \x0A \x0D \x5C \x27 — bytes that need escaping in plain string literals.
		err := iw.Write([][]byte{{0x00, 0x0A, 0x0D, 0x5C, 0x27}})
		assert.NoError(t, err)
		assert.Equal(t, "(X'000A0D5C27')\n", buf.String())
	})

	t.Run("null_binary_value", func(t *testing.T) {
		table := core.Table{
			Name:    "t",
			Columns: []core.Column{binaryCol(dbmsdriver.TypeBlob)},
		}
		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, true)
		err := iw.Write([][]byte{core.NullValueSeq})
		assert.NoError(t, err)
		assert.Equal(t, "(NULL)\n", buf.String())
	})

	t.Run("empty_binary_value", func(t *testing.T) {
		table := core.Table{
			Name:    "t",
			Columns: []core.Column{binaryCol(dbmsdriver.TypeBlob)},
		}
		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, true)
		err := iw.Write([][]byte{{}})
		assert.NoError(t, err)
		assert.Equal(t, "(X'')\n", buf.String())
	})

	t.Run("mixed_binary_and_text_columns", func(t *testing.T) {
		table := core.Table{
			Name: "t",
			Columns: []core.Column{
				{Name: "id", TypeName: dbmsdriver.TypeInt, TypeClass: core.TypeClassInt},
				{Name: "name", TypeName: dbmsdriver.TypeVarChar, TypeClass: core.TypeClassText},
				{Name: "data", TypeName: dbmsdriver.TypeBlob, TypeClass: core.TypeClassBinary},
			},
		}
		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, true)
		err := iw.Write([][]byte{
			[]byte("42"),
			[]byte("hello"),
			{0xCA, 0xFE, 0xBA, 0xBE},
		})
		assert.NoError(t, err)
		assert.Equal(t, "('42', 'hello', X'CAFEBABE')\n", buf.String())
	})

	t.Run("hex_blob_disabled_binary_col_is_string", func(t *testing.T) {
		table := core.Table{
			Name:    "t",
			Columns: []core.Column{binaryCol(dbmsdriver.TypeBlob)},
		}
		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, false)
		// With hex-blob off, binary data goes through the string/escape path.
		err := iw.Write([][]byte{{'A', 'B', 'C'}})
		assert.NoError(t, err)
		assert.Equal(t, "('ABC')\n", buf.String())
	})
}
