package streamers

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/rawrecord"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
)

func TestInsertWriter_Write(t *testing.T) {

	t.Run("single_insert_per_write", func(t *testing.T) {
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
		iw := NewInsertWriter(table, &buf, DefaultInsertBatchSize)

		rows := [][][]byte{
			{[]byte("1"), []byte("John Doe"), []byte("john@example.com")},
			{[]byte("2"), []byte("Jane Doe"), []byte("jane@example.com")},
		}

		err := iw.Write(rows[0])
		assert.NoError(t, err)
		err = iw.Write(rows[1])
		assert.NoError(t, err)
		err = iw.Flush()
		assert.NoError(t, err)

		expected := "INSERT INTO `public`.`users` (`id`, `name`, `email`) VALUES \n('1', 'John Doe', 'john@example.com'),\n('2', 'Jane Doe', 'jane@example.com');\n"
		assert.Equal(t, expected, buf.String())
	})

	t.Run("escaping_identifiers", func(t *testing.T) {
		table := models.Table{
			Name: "order items",
			Columns: []models.Column{
				{Name: "order id"},
				{Name: "product-name"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, DefaultInsertBatchSize)

		row := [][]byte{[]byte("101"), []byte("Widget")}
		err := iw.Write(row)
		assert.NoError(t, err)
		err = iw.Flush()
		assert.NoError(t, err)

		expected := "INSERT INTO `order items` (`order id`, `product-name`) VALUES \n('101', 'Widget');\n"
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
		iw := NewInsertWriter(table, &buf, DefaultInsertBatchSize)

		row := [][]byte{[]byte("1"), nil}
		err := iw.Write(row)
		assert.NoError(t, err)
		err = iw.Flush()
		assert.NoError(t, err)

		expected := "INSERT INTO `test` (`id`, `val`) VALUES \n('1', NULL);\n"
		assert.Equal(t, expected, buf.String())
	})

	t.Run("null_value_sequence", func(t *testing.T) {
		table := models.Table{
			Name: "test",
			Columns: []models.Column{
				{Name: "id"},
				{Name: "val"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, DefaultInsertBatchSize)

		row := [][]byte{[]byte("1"), dbmsdriver.NullValueSeq}
		err := iw.Write(row)
		assert.NoError(t, err)
		err = iw.Flush()
		assert.NoError(t, err)

		expected := "INSERT INTO `test` (`id`, `val`) VALUES \n('1', NULL);\n"
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
		iw := NewInsertWriter(table, &buf, DefaultInsertBatchSize)

		// Use RawRecord to prepare the row, ensuring consistent escaping logic
		rr := rawrecord.NewRawRecord(2, dbmsdriver.NullValueSeq)
		err := rr.SetColumn(0, models.NewColumnRawValue([]byte("1"), false))
		assert.NoError(t, err)
		// Set literal string matching NULL sequence ("\N")
		err = rr.SetColumn(1, models.NewColumnRawValue(dbmsdriver.NullValueSeq, false))
		assert.NoError(t, err)

		err = iw.Write(rr.GetRow())
		assert.NoError(t, err)
		err = iw.Flush()
		assert.NoError(t, err)

		// Expected SQL should have the literal string "\N" escaped.
		// RawRecord escapes "\N" (1 backslash) to "\\N" (2 backslashes).
		// go-sqlbuilder for MySQL escapes each backslash in the string to two.
		// So "\\N" (2 backslashes) becomes "\\\\N" (4 backslashes) in the SQL.
		// In Go string literal, 4 backslashes are written as 8 backslashes.
		expected := "INSERT INTO `test` (`id`, `val`) VALUES \n('1', '\\\\\\\\N');\n"
		assert.Equal(t, expected, buf.String())
	})

	t.Run("escaping_values", func(t *testing.T) {
		table := models.Table{
			Name: "products",
			Columns: []models.Column{
				{Name: "name"},
				{Name: "description"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, DefaultInsertBatchSize)

		row := [][]byte{
			[]byte("O'Reilly's Book"),
			[]byte("Backslash \\ and Quote ' and Newline \n"),
		}
		err := iw.Write(row)
		assert.NoError(t, err)
		err = iw.Flush()
		assert.NoError(t, err)

		output := buf.String()
		// go-sqlbuilder for MySQL uses backslash escaping (\', \\, \n, etc.)
		assert.Contains(t, output, "'O\\'Reilly\\'s Book'")
		assert.Contains(t, output, "'Backslash \\\\ and Quote \\' and Newline \\n'")
		assert.True(t, strings.HasSuffix(output, ";\n"))
	})

	t.Run("no_writes_for_empty_table", func(t *testing.T) {
		table := models.Table{
			Name: "empty_table",
			Columns: []models.Column{
				{Name: "id"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, DefaultInsertBatchSize)

		err := iw.Flush()
		assert.NoError(t, err)

		assert.Empty(t, buf.String(), "Expected buffer to be empty for no writes")
	})

	t.Run("chunked_inserts", func(t *testing.T) {
		table := models.Table{
			Schema: "public",
			Name:   "users",
			Columns: []models.Column{
				{Name: "id"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, 2)

		rows := [][][]byte{
			{[]byte("1")},
			{[]byte("2")},
			{[]byte("3")},
		}

		err := iw.Write(rows[0])
		assert.NoError(t, err)
		err = iw.Write(rows[1])
		assert.NoError(t, err)
		err = iw.Write(rows[2])
		assert.NoError(t, err)
		err = iw.Flush()
		assert.NoError(t, err)

		// batchSize=2, so first two rows in one insert, third in another
		expected := "INSERT INTO `public`.`users` (`id`) VALUES \n('1'),\n('2');\nINSERT INTO `public`.`users` (`id`) VALUES \n('3');\n"
		assert.Equal(t, expected, buf.String())
	})

	t.Run("one_insert_per_row_batch_size_zero", func(t *testing.T) {
		table := models.Table{
			Schema: "public",
			Name:   "users",
			Columns: []models.Column{
				{Name: "id"},
			},
		}

		var buf bytes.Buffer
		iw := NewInsertWriter(table, &buf, 0)

		rows := [][][]byte{
			{[]byte("1")},
			{[]byte("2")},
		}

		err := iw.Write(rows[0])
		assert.NoError(t, err)
		err = iw.Write(rows[1])
		assert.NoError(t, err)
		err = iw.Flush()
		assert.NoError(t, err)

		// batchSize=0 implies one statement per row
		expected := "INSERT INTO `public`.`users` (`id`) VALUES \n('1');\nINSERT INTO `public`.`users` (`id`) VALUES \n('2');\n"
		assert.Equal(t, expected, buf.String())
	})
}
