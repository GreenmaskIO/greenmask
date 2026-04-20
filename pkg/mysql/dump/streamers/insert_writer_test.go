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
		iw := NewInsertWriter(table, &buf)

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
		iw := NewInsertWriter(table, &buf)

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
		iw := NewInsertWriter(table, &buf)

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
		iw := NewInsertWriter(table, &buf)

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
		iw := NewInsertWriter(table, &buf)

		err := iw.Flush()
		assert.NoError(t, err)
		assert.Empty(t, buf.String())
	})
}
