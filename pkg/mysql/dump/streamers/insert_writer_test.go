package streamers

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/rawrecord"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
)

func TestInsertWriter_Write(t *testing.T) {

	t.Run("single_row", func(t *testing.T) {
		table := models.Table{
			Schema: "public",
			Name:   "users",
			Columns: []models.Column{
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
		table := models.Table{
			Schema: "public",
			Name:   "users",
			Columns: []models.Column{
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
		table := models.Table{
			Name: "test",
			Columns: []models.Column{
				{Name: "id"},
				{Name: "val"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, false)

		row := [][]byte{[]byte("1"), dbmsdriver.NullValueSeq}
		err := iw.Write(row)
		assert.NoError(t, err)

		expected := "('1', NULL)\n"
		assert.Equal(t, expected, buf.String())
	})

	t.Run("literal_null_sequence", func(t *testing.T) {
		table := models.Table{
			Name: "test",
			Columns: []models.Column{
				{Name: "id"},
				{Name: "val"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, false)

		rr := rawrecord.NewRawRecord(2, dbmsdriver.NullValueSeq)
		err := rr.SetColumn(0, models.NewColumnRawValue([]byte("1"), false))
		assert.NoError(t, err)
		err = rr.SetColumn(1, models.NewColumnRawValue(dbmsdriver.NullValueSeq, false))
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
		table := models.Table{
			Name:    "empty_table",
			Columns: []models.Column{{Name: "id"}},
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
	binaryCol := func(typeName string) models.Column {
		return models.Column{Name: "data", TypeName: typeName, TypeClass: models.TypeClassBinary}
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
			table := models.Table{
				Name:    "t",
				Columns: []models.Column{binaryCol(typeName)},
			}
			iw := NewInsertWriter(table, &buf, true)
			err := iw.Write([][]byte{{0xDE, 0xAD, 0xBE, 0xEF}})
			assert.NoError(t, err, "type %s", typeName)
			assert.Equal(t, "(X'DEADBEEF')\n", buf.String(), "type %s", typeName)
		}
	})

	t.Run("high_bytes_invalid_utf8", func(t *testing.T) {
		table := models.Table{
			Name:    "t",
			Columns: []models.Column{binaryCol(dbmsdriver.TypeBlob)},
		}
		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, true)
		// Bytes > 0x7F that are invalid utf8mb4 sequences — unsafe without hex-blob.
		err := iw.Write([][]byte{{0x80, 0x81, 0xFE, 0xFF}})
		assert.NoError(t, err)
		assert.Equal(t, "(X'8081FEFF')\n", buf.String())
	})

	t.Run("null_byte_and_escape_chars", func(t *testing.T) {
		table := models.Table{
			Name:    "t",
			Columns: []models.Column{binaryCol(dbmsdriver.TypeVarBinary)},
		}
		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, true)
		// \x00 \x0A \x0D \x5C \x27 — bytes that need escaping in plain string literals.
		err := iw.Write([][]byte{{0x00, 0x0A, 0x0D, 0x5C, 0x27}})
		assert.NoError(t, err)
		assert.Equal(t, "(X'000A0D5C27')\n", buf.String())
	})

	t.Run("null_binary_value", func(t *testing.T) {
		table := models.Table{
			Name:    "t",
			Columns: []models.Column{binaryCol(dbmsdriver.TypeBlob)},
		}
		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, true)
		err := iw.Write([][]byte{dbmsdriver.NullValueSeq})
		assert.NoError(t, err)
		assert.Equal(t, "(NULL)\n", buf.String())
	})

	t.Run("empty_binary_value", func(t *testing.T) {
		table := models.Table{
			Name:    "t",
			Columns: []models.Column{binaryCol(dbmsdriver.TypeBlob)},
		}
		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, true)
		err := iw.Write([][]byte{{}})
		assert.NoError(t, err)
		assert.Equal(t, "(X'')\n", buf.String())
	})

	t.Run("mixed_binary_and_text_columns", func(t *testing.T) {
		table := models.Table{
			Name: "t",
			Columns: []models.Column{
				{Name: "id", TypeName: dbmsdriver.TypeInt, TypeClass: models.TypeClassInt},
				{Name: "name", TypeName: dbmsdriver.TypeVarChar, TypeClass: models.TypeClassText},
				{Name: "data", TypeName: dbmsdriver.TypeBlob, TypeClass: models.TypeClassBinary},
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
		table := models.Table{
			Name:    "t",
			Columns: []models.Column{binaryCol(dbmsdriver.TypeBlob)},
		}
		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, false)
		// With hex-blob off, binary data goes through the string/escape path.
		err := iw.Write([][]byte{{'A', 'B', 'C'}})
		assert.NoError(t, err)
		assert.Equal(t, "('ABC')\n", buf.String())
	})
}
