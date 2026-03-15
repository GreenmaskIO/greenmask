package rawrecord

import (
	"bytes"
	"testing"

	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/stretchr/testify/assert"
)

func TestRawRecord_Escaping(t *testing.T) {
	nullSeq := []byte("\\N")
	rr := NewRawRecord(2, nullSeq)

	t.Run("set_null", func(t *testing.T) {
		err := rr.SetColumn(0, models.NewColumnRawValue(nil, true))
		assert.NoError(t, err)

		val, err := rr.GetColumn(0)
		assert.NoError(t, err)
		assert.True(t, val.IsNull)
		assert.Nil(t, val.Data)
		assert.True(t, bytes.Equal(rr.row[0], nullSeq))
	})

	t.Run("set_string_matching_null_seq", func(t *testing.T) {
		// Literal string "\N"
		err := rr.SetColumn(1, models.NewColumnRawValue(nullSeq, false))
		assert.NoError(t, err)

		val, err := rr.GetColumn(1)
		assert.NoError(t, err)
		assert.False(t, val.IsNull)
		assert.Equal(t, nullSeq, val.Data)

		// Internal storage should be escaped
		assert.True(t, bytes.Equal(rr.row[1], append([]byte("\\"), nullSeq...)))
	})

	t.Run("set_regular_string", func(t *testing.T) {
		data := []byte("regular string")
		err := rr.SetColumn(0, models.NewColumnRawValue(data, false))
		assert.NoError(t, err)

		val, err := rr.GetColumn(0)
		assert.NoError(t, err)
		assert.False(t, val.IsNull)
		assert.Equal(t, data, val.Data)
	})

	t.Run("get_escaped_null_seq", func(t *testing.T) {
		escapedNull := append([]byte("\\"), nullSeq...)
		err := rr.SetRow([][]byte{escapedNull, []byte("other")})
		assert.NoError(t, err)

		val, err := rr.GetColumn(0)
		assert.NoError(t, err)
		assert.False(t, val.IsNull)
		assert.Equal(t, nullSeq, val.Data)
	})

	t.Run("set_string_starting_with_backslash_but_not_null_seq", func(t *testing.T) {
		data := []byte("\\NOT_NULL")
		err := rr.SetColumn(0, models.NewColumnRawValue(data, false))
		assert.NoError(t, err)

		val, err := rr.GetColumn(0)
		assert.NoError(t, err)
		assert.False(t, val.IsNull)
		assert.Equal(t, data, val.Data)

		// Should NOT be escaped further because it's not the exact null sequence
		assert.True(t, bytes.Equal(rr.row[0], data))
	})
}
