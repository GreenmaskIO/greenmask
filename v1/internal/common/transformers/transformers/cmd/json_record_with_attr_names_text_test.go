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
)

func TestJsonRecordWithAttrNames_Encode(t *testing.T) {
	t.Run("success text", func(t *testing.T) {
		record := NewJsonRecordWithAttrNames[*JsonAttrRawValueText](NewJsonAttrRawValueText)
		val1 := commonmodels.NewColumnRawValue([]byte("value1"), false)
		val2 := commonmodels.NewColumnRawValue([]byte("value2"), false)
		column1 := &commonmodels.Column{
			Idx:  1,
			Name: "column1",
		}
		column2 := &commonmodels.Column{
			Idx:  2,
			Name: "column2",
		}
		err := record.SetColumn(column1, val1)
		require.NoError(t, err)
		err = record.SetColumn(column2, val2)
		require.NoError(t, err)

		encoded, err := record.Encode()
		require.NoError(t, err)

		expected := `{"column1":{"d":"value1","n":false},"column2":{"d":"value2","n":false}}`
		require.JSONEq(t, expected, string(encoded))
	})

	t.Run("success bytes", func(t *testing.T) {
		record := NewJsonRecordWithAttrNames[*JsonAttrRawValueBytes](NewJsonAttrRawValueBytes)
		val1 := commonmodels.NewColumnRawValue([]byte("value1"), false)
		val2 := commonmodels.NewColumnRawValue([]byte("value2"), false)
		column1 := &commonmodels.Column{
			Idx:  1,
			Name: "column1",
		}
		column2 := &commonmodels.Column{
			Idx:  2,
			Name: "column2",
		}
		err := record.SetColumn(column1, val1)
		require.NoError(t, err)
		err = record.SetColumn(column2, val2)
		require.NoError(t, err)

		encoded, err := record.Encode()
		require.NoError(t, err)

		expected := `{"column1":{"d":"dmFsdWUx","n":false},"column2":{"d":"dmFsdWUy","n":false}}`
		require.JSONEq(t, expected, string(encoded))
	})
}

func TestJsonRecordWithAttrNames_Decode(t *testing.T) {
	t.Run("success text", func(t *testing.T) {
		rawData := []byte(`{"column1":{"d":"value1","n":false},"column2":{"d":"value2","n":false}}`)
		record := NewJsonRecordWithAttrNames[*JsonAttrRawValueText](NewJsonAttrRawValueText)

		err := record.Decode(rawData)
		require.NoError(t, err)

		column1 := &commonmodels.Column{
			Idx:  1,
			Name: "column1",
		}
		column2 := &commonmodels.Column{
			Idx:  2,
			Name: "column2",
		}

		val1, err := record.GetColumn(column1)
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), val1.Data)
		require.False(t, val1.IsNull)

		val2, err := record.GetColumn(column2)
		require.NoError(t, err)
		require.Equal(t, []byte("value2"), val2.Data)
		require.False(t, val2.IsNull)
	})

	t.Run("success bytes", func(t *testing.T) {
		rawData := []byte(`{"column1":{"d":"dmFsdWUx","n":false},"column2":{"d":"dmFsdWUy","n":false}}`)
		record := NewJsonRecordWithAttrNames[*JsonAttrRawValueBytes](NewJsonAttrRawValueBytes)

		err := record.Decode(rawData)
		require.NoError(t, err)

		column1 := &commonmodels.Column{
			Idx:  1,
			Name: "column1",
		}
		column2 := &commonmodels.Column{
			Idx:  2,
			Name: "column2",
		}

		val1, err := record.GetColumn(column1)
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), val1.Data)
		require.False(t, val1.IsNull)

		val2, err := record.GetColumn(column2)
		require.NoError(t, err)
		require.Equal(t, []byte("value2"), val2.Data)
		require.False(t, val2.IsNull)
	})
}
