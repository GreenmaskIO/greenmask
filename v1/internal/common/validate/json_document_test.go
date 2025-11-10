package validate

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	ttesting "github.com/greenmaskio/greenmask/v1/internal/common/transformers/testing"
)

func TestJsonDocument_GetAffectedColumns(t *testing.T) {
	tab, _, _ := getTableAndRows(t)
	affectedColumns := []int{3}
	jd := NewJsonDocument(tab, affectedColumns, true, true)
	colsToPrint := jd.GetColumnsToPrint()
	assert.Len(t, colsToPrint, 2)
	assert.Equal(t, map[int]struct{}{0: {}, 3: {}}, colsToPrint)
}

func TestJsonDocument_GetRecords(t *testing.T) {
	t.Run("only transformed", func(t *testing.T) {
		tab, originalRecs, transformedRecs := getTableAndRows(t)
		affectedColumns := []int{3}

		jd := NewJsonDocument(tab, affectedColumns, true, true)

		for i := range originalRecs {
			err := jd.Append(originalRecs[i], transformedRecs[i])
			require.NoError(t, err)
		}
		columnsToPrint := jd.GetColumnsToPrint()
		assert.Equal(t, map[int]struct{}{0: {}, 2: {}, 3: {}}, columnsToPrint)

		columnsImplicitlyChanged := jd.GetUnexpectedlyChangedColumns()
		assert.Equal(t, map[int]struct{}{2: {}}, columnsImplicitlyChanged)

		actual := jd.Get()
		expected := JsonDocumentResult{
			Schema: "playground",
			Name:   "users",
			PrimaryKeyColumns: []string{
				"id",
			},
			WithDiff:        true,
			OnlyTransformed: true,
			RecordsWithDiff: []jsonRecordWithDiff{
				{
					"created_at": valueWithDiff{
						ColNum:      3,
						Original:    "2022-05-05 10:15:30.123",
						Transformed: "2022-05-01 09:00:00.123",
						Expected:    true,
					},
					"email": valueWithDiff{
						ColNum:      2,
						Original:    "alice@example",
						Transformed: "test@example",
						Equal:       false,
						Expected:    false,
					},
					"id": valueWithDiff{
						ColNum:      0,
						Original:    "1",
						Transformed: "1",
						Equal:       true,
						Expected:    true,
					},
				},
			},
		}
		diff := cmp.Diff(expected, actual)
		if diff != "" {
			t.Errorf("mismatch (-expected +actual):\n%s", diff)
		}
	})

	t.Run("all", func(t *testing.T) {
		tab, originalRecs, transformedRecs := getTableAndRows(t)
		affectedColumns := []int{3}

		jd := NewJsonDocument(tab, affectedColumns, true, false)

		for i := range originalRecs {
			err := jd.Append(originalRecs[i], transformedRecs[i])
			require.NoError(t, err)
		}
		columnsToPrint := jd.GetColumnsToPrint()
		assert.Equal(t, map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}}, columnsToPrint)

		columnsImplicitlyChanged := jd.GetUnexpectedlyChangedColumns()
		assert.Equal(t, map[int]struct{}{2: {}}, columnsImplicitlyChanged)

		actual := jd.Get()
		expected := JsonDocumentResult{
			Schema: "playground",
			Name:   "users",
			PrimaryKeyColumns: []string{
				"id",
			},
			WithDiff:        true,
			OnlyTransformed: false,
			RecordsWithDiff: []jsonRecordWithDiff{
				{
					"created_at": valueWithDiff{
						ColNum:      3,
						Original:    "2022-05-05 10:15:30.123",
						Transformed: "2022-05-01 09:00:00.123",
						Expected:    true,
					},
					"email": valueWithDiff{
						ColNum:      2,
						Original:    "alice@example",
						Transformed: "test@example",
						Equal:       false,
						Expected:    false,
					},
					"id": valueWithDiff{
						ColNum:      0,
						Original:    "1",
						Transformed: "1",
						Equal:       true,
						Expected:    true,
					},
					"username": valueWithDiff{
						ColNum:      1,
						Original:    "alice",
						Transformed: "alice",
						Equal:       true,
						Expected:    true,
					},
				},
			},
		}
		diff := cmp.Diff(expected, actual)
		if diff != "" {
			t.Errorf("mismatch (-expected +actual):\n%s", diff)
		}
	})
}

func getTableAndRows(t *testing.T) (commonmodels.Table, []interfaces.RowDriver, []interfaces.RowDriver) {
	t.Helper()

	tableDef := `
		{
		  "id": 1,
		  "schema": "playground",
		  "name": "users",
		  "columns": [
			{
			  "idx": 0,
			  "name": "id",
			  "type_name": "int",
			  "type_oid": 3,
			  "TypeClass": "int",
			  "not_null": true,
			  "length": 0,
			  "size": 0
			},
			{
			  "idx": 1,
			  "name": "username",
			  "type_name": "varchar(50)",
			  "type_oid": 17,
			  "TypeClass": "text",
			  "not_null": true,
			  "length": 0,
			  "size": 0
			},
			{
			  "idx": 2,
			  "name": "email",
			  "type_name": "varchar(100)",
			  "type_oid": 17,
			  "TypeClass": "text",
			  "not_null": true,
			  "length": 0,
			  "size": 0
			},
			{
			  "idx": 3,
			  "name": "created_at",
			  "type_name": "datetime",
			  "type_oid": 12,
			  "TypeClass": "datetime",
			  "not_null": true,
			  "length": 0,
			  "size": 0
			}
		  ],
		  "size": 0,
		  "primary_key": [
			"id"
		  ]
		}
	`
	originalData := ttesting.NewDummyRow(4)
	err := originalData.SetColumn(0, commonmodels.NewColumnRawValue([]byte("1"), false))
	require.NoError(t, err)
	err = originalData.SetColumn(1, commonmodels.NewColumnRawValue([]byte("alice"), false))
	require.NoError(t, err)
	err = originalData.SetColumn(2, commonmodels.NewColumnRawValue([]byte("alice@example"), false))
	require.NoError(t, err)
	err = originalData.SetColumn(3, commonmodels.NewColumnRawValue([]byte("2022-05-05 10:15:30.123"), false))
	require.NoError(t, err)

	transformedData := ttesting.NewDummyRow(4)
	err = transformedData.SetColumn(0, commonmodels.NewColumnRawValue([]byte("1"), false))
	require.NoError(t, err)
	err = transformedData.SetColumn(1, commonmodels.NewColumnRawValue([]byte("alice"), false))
	require.NoError(t, err)
	err = transformedData.SetColumn(2, commonmodels.NewColumnRawValue([]byte("test@example"), false))
	require.NoError(t, err)
	err = transformedData.SetColumn(3, commonmodels.NewColumnRawValue([]byte("2022-05-01 09:00:00.123"), false))
	require.NoError(t, err)

	var tab commonmodels.Table
	err = json.Unmarshal([]byte(tableDef), &tab)
	require.NoError(nil, err)

	return tab, []interfaces.RowDriver{originalData}, []interfaces.RowDriver{transformedData}
}
