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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTextRecord_Encode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		columns := []core.Column{
			{
				Idx:      0,
				Name:     "first_name",
				TypeName: mysqldbmsdriver.TypeText,
				TypeID:   mysqldbmsdriver.TypeIDText,
				Length:   0,
			},
		}
		transferColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		affectedColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		record, err := NewTextRecord(
			transferColumn,
			affectedColumn,
		)
		require.NoError(t, err)
		val1 := core.NewColumnRawValue([]byte("value1"), false)
		err = record.SetColumn(&columns[0], val1)
		require.NoError(t, err)

		encoded, err := record.Encode()
		require.NoError(t, err)

		expected := "value1"
		assert.Equal(t, expected, string(encoded))
	})
}

func TestTextRecord_Decode(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		rawData := []byte("value1")
		columns := []core.Column{
			{
				Idx:  0,
				Name: "first_name",
			},
			{
				Idx:  1,
				Name: "last_name",
			},
		}
		transferColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		affectedColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		record, err := NewTextRecord(
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

		_, err = record.GetColumn(&columns[1])
		require.Error(t, err)
		require.ErrorIs(t, err, errUnexpectedColumn)
	})
}
