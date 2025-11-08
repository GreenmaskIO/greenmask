package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestCSVRecord_Encode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		columns := []commonmodels.Column{
			{
				Idx:      0,
				Name:     "first_name",
				TypeName: mysqldbmsdriver.TypeText,
				TypeOID:  mysqldbmsdriver.VirtualOidText,
				Length:   0,
			},
			{
				Idx:      1,
				Name:     "last_name",
				TypeName: mysqldbmsdriver.TypeText,
				TypeOID:  mysqldbmsdriver.VirtualOidText,
				Length:   0,
			},
			{
				Idx:      2,
				Name:     "middle_name",
				TypeName: mysqldbmsdriver.TypeText,
				TypeOID:  mysqldbmsdriver.VirtualOidText,
				Length:   0,
			},
		}
		transferColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
			{
				Column:   &columns[2],
				Position: 1,
			},
		}
		affectedColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		record, err := NewCSVRecord(
			transferColumn,
			affectedColumn,
		)
		require.NoError(t, err)
		val1 := commonmodels.NewColumnRawValue([]byte("value1"), false)
		val2 := commonmodels.NewColumnRawValue([]byte("value2"), false)
		err = record.SetColumn(&columns[0], val1)
		require.NoError(t, err)
		err = record.SetColumn(&columns[2], val2)
		require.NoError(t, err)

		encoded, err := record.Encode()
		require.NoError(t, err)

		expected := "value1,value2\n"
		assert.Equal(t, expected, string(encoded))
	})
}

func TestCSVRecord_Decode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		rawData := []byte("value1,value2\n")
		columns := []commonmodels.Column{
			{
				Idx:  0,
				Name: "first_name",
			},
			{
				Idx:  1,
				Name: "last_name",
			},
			{
				Idx:  2,
				Name: "middle_name",
			},
		}
		transferColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
			{
				Column:   &columns[2],
				Position: 1,
			},
		}
		affectedColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		record, err := NewCSVRecord(
			transferColumn,
			affectedColumn,
		)
		require.NoError(t, err)

		err = record.Decode(rawData)
		require.NoError(t, err)

		val1, err := record.GetColumn(&columns[0])
		require.NoError(t, err)
		assert.Equal(t, []byte("value1"), val1.Data)
		assert.False(t, val1.IsNull)

		_, err = record.GetColumn(&columns[2])
		require.Error(t, err)
		require.ErrorIs(t, err, errUnexpectedColumn)
	})
}
