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

package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestJsonRecordWithAttrIndexes_Encode(t *testing.T) {
	t.Run("success text", func(t *testing.T) {
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
		record := NewJsonRecordWithAttrIndexes[*JsonAttrRawValueText](
			transferColumn,
			affectedColumn,
			NewJsonAttrRawValueText,
		)
		val1 := commonmodels.NewColumnRawValue([]byte("value1"), false)
		val2 := commonmodels.NewColumnRawValue([]byte("value2"), false)
		err := record.SetColumn(&columns[0], val1)
		require.NoError(t, err)
		err = record.SetColumn(&columns[2], val2)
		require.NoError(t, err)

		encoded, err := record.Encode()
		require.NoError(t, err)

		expected := `[{"d":"value1","n":false},{"d":"value2","n":false}]`
		require.JSONEq(t, expected, string(encoded))
	})

	t.Run("success bytes", func(t *testing.T) {
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
		record := NewJsonRecordWithAttrIndexes[*JsonAttrRawValueBytes](
			transferColumn,
			affectedColumn,
			NewJsonAttrRawValueBytes,
		)
		val1 := commonmodels.NewColumnRawValue([]byte("value1"), false)
		val2 := commonmodels.NewColumnRawValue([]byte("value2"), false)
		err := record.SetColumn(&columns[0], val1)
		require.NoError(t, err)
		err = record.SetColumn(&columns[2], val2)
		require.NoError(t, err)

		encoded, err := record.Encode()
		require.NoError(t, err)

		expected := `[{"d":"dmFsdWUx","n":false},{"d":"dmFsdWUy","n":false}]`
		require.JSONEq(t, expected, string(encoded))
	})
}

func TestJsonRecordWithAttrIndexes_Decode(t *testing.T) {
	t.Run("success text", func(t *testing.T) {
		rawData := []byte(`[{"d":"value1","n":false},{"d":"value2","n":false}]`)
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
		record := NewJsonRecordWithAttrIndexes[*JsonAttrRawValueText](
			transferColumn,
			affectedColumn,
			NewJsonAttrRawValueText,
		)

		err := record.Decode(rawData)
		require.NoError(t, err)

		val1, err := record.GetColumn(&columns[0])
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), val1.Data)
		require.False(t, val1.IsNull)

		_, err = record.GetColumn(&columns[2])
		require.Error(t, err)
		require.ErrorIs(t, err, errUnexpectedColumn)
	})

	t.Run("success bytes", func(t *testing.T) {
		rawData := []byte(`[{"d":"dmFsdWUx","n":false},{"d":"dmFsdWUy","n":false}]`)
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
		record := NewJsonRecordWithAttrIndexes[*JsonAttrRawValueBytes](
			transferColumn,
			affectedColumn,
			NewJsonAttrRawValueBytes,
		)

		err := record.Decode(rawData)
		require.NoError(t, err)

		val1, err := record.GetColumn(&columns[0])
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), val1.Data)
		require.False(t, val1.IsNull)

		_, err = record.GetColumn(&columns[2])
		require.Error(t, err)
		require.ErrorIs(t, err, errUnexpectedColumn)
	})
}
