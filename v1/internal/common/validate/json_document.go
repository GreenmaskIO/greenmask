package validate

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type Documenter interface {
	Print(w io.Writer) error
	Append(original, transformed [][]byte) error
}

type valueWithDiff struct {
	ColNum      int    `json:"-"`
	Original    string `json:"original"`
	Transformed string `json:"transformed"`
	Equal       bool   `json:"equal"`
	Expected    bool   `json:"implicit"`
}

type JsonDocumentResult struct {
	Schema            string
	Name              string
	PrimaryKeyColumns []string
	WithDiff          bool
	OnlyTransformed   bool
	RecordsWithDiff   []jsonRecordWithDiff
	RecordsPlain      []jsonRecordPlain
}

type jsonDocumentResponseWithDiff struct {
	Schema            string               `json:"schema"`
	Name              string               `json:"name"`
	PrimaryKeyColumns []string             `json:"primary_key_columns"`
	WithDiff          bool                 `json:"with_diff"`
	TransformedOnly   bool                 `json:"transformed_only"`
	Records           []jsonRecordWithDiff `json:"records"`
}

type jsonDocumentResponsePlain struct {
	Schema            string            `json:"schema"`
	Name              string            `json:"name"`
	PrimaryKeyColumns []string          `json:"primary_key_columns"`
	WithDiff          bool              `json:"with_diff"`
	TransformedOnly   bool              `json:"transformed_only"`
	Records           []jsonRecordPlain `json:"records"`
}

type jsonRecordWithDiff map[string]valueWithDiff

type jsonRecordPlain map[string]string

type JsonDocument struct {
	result                    JsonDocumentResult
	table                     commonmodels.Table
	withDiff                  bool
	expectedAffectedColumns   map[int]struct{}
	unexpectedAffectedColumns map[int]struct{}
	onlyTransformed           bool
}

func NewJsonDocument(
	table commonmodels.Table,
	affectedColumns []int,
	withDiff bool,
	onlyTransformed bool,
) *JsonDocument {
	expectedAffectedColumns := map[int]struct{}{}
	for _, colIdx := range affectedColumns {
		expectedAffectedColumns[colIdx] = struct{}{}
	}
	return &JsonDocument{
		result: JsonDocumentResult{
			Schema:            table.Schema,
			Name:              table.Name,
			PrimaryKeyColumns: table.PrimaryKey,
			WithDiff:          withDiff,
			OnlyTransformed:   onlyTransformed,
			RecordsWithDiff:   make([]jsonRecordWithDiff, 0),
		},
		withDiff:                  withDiff,
		table:                     table,
		expectedAffectedColumns:   expectedAffectedColumns,
		unexpectedAffectedColumns: make(map[int]struct{}),
		onlyTransformed:           onlyTransformed,
	}
}

func (jc *JsonDocument) Append(original, transformed interfaces.RowDriver) error {
	r := make(jsonRecordWithDiff)
	for i := range jc.table.Columns {
		col := jc.table.Columns[i]
		originalRawValue, err := original.GetColumn(col.Idx)
		if err != nil {
			return fmt.Errorf("get value from original record: %w", err)
		}

		var originalValue, transformedValue string

		originalValue = getStringFromRawValue(originalRawValue)

		expected := true

		transformedRawValue, err := transformed.GetColumn(col.Idx)
		if err != nil {
			return fmt.Errorf("get value from transformed record: %w", err)
		}
		transformedValue = getStringFromRawValue(transformedRawValue)
		equal := ValuesEqual(originalRawValue, transformedRawValue)
		if _, ok := jc.expectedAffectedColumns[col.Idx]; !equal && !ok {
			expected = false
			jc.unexpectedAffectedColumns[col.Idx] = struct{}{}
		}

		r[col.Name] = valueWithDiff{
			Original:    originalValue,
			Transformed: transformedValue,
			Equal:       equal,
			Expected:    expected,
			ColNum:      col.Idx,
		}
	}
	jc.result.RecordsWithDiff = append(jc.result.RecordsWithDiff, r)
	return nil
}

func (jc *JsonDocument) Marshall() ([]byte, error) {
	result := jc.Get()

	if result.WithDiff {
		response := &jsonDocumentResponseWithDiff{
			Schema:            result.Schema,
			Name:              result.Name,
			PrimaryKeyColumns: result.PrimaryKeyColumns,
			WithDiff:          result.WithDiff,
			TransformedOnly:   result.OnlyTransformed,
			Records:           result.RecordsWithDiff,
		}
		res, err := json.Marshal(response)
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	records := make([]jsonRecordPlain, len(result.RecordsWithDiff))

	for idx := range records {
		record := make(map[string]string, len(result.RecordsWithDiff[idx]))
		for name, value := range result.RecordsWithDiff[idx] {
			record[name] = value.Transformed
		}
		records = append(records, record)
	}

	response := &jsonDocumentResponsePlain{
		Schema:            result.Schema,
		Name:              result.Name,
		PrimaryKeyColumns: result.PrimaryKeyColumns,
		WithDiff:          result.WithDiff,
		TransformedOnly:   result.OnlyTransformed,
		Records:           records,
	}
	res, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (jc *JsonDocument) GetUnexpectedlyChangedColumns() map[int]struct{} {
	return jc.unexpectedAffectedColumns
}

func (jc *JsonDocument) GetAffectedColumns() map[int]struct{} {
	affectedColumns := maps.Clone(jc.expectedAffectedColumns)
	maps.Copy(affectedColumns, jc.unexpectedAffectedColumns)
	return affectedColumns
}

func (jc *JsonDocument) GetColumnsToPrint() map[int]struct{} {
	if jc.onlyTransformed {
		columnsToPrint := jc.GetAffectedColumns()
		for _, colName := range jc.result.PrimaryKeyColumns {
			idx := slices.IndexFunc(jc.table.Columns, func(column commonmodels.Column) bool {
				return column.Name == colName
			})
			if idx == -1 {
				panic("primary key column not found in table columns")
			}
			columnsToPrint[idx] = struct{}{}
		}
		return columnsToPrint
	}

	columnsToPrint := make(map[int]struct{}, len(jc.table.Columns))
	for _, c := range jc.table.Columns {
		columnsToPrint[c.Idx] = struct{}{}
	}
	return columnsToPrint
}

func (jc *JsonDocument) Get() JsonDocumentResult {
	if jc.onlyTransformed {
		jc.filterColumns()
	}
	return jc.result
}

func (jc *JsonDocument) filterColumns() {
	// Determine list of the affected columns
	columnsToPrint := jc.GetColumnsToPrint()
	columnsToDelete := make(map[string]struct{})
	for _, c := range jc.table.Columns {
		if _, ok := columnsToPrint[c.Idx]; !ok {
			columnsToDelete[c.Name] = struct{}{}
		}
	}

	for _, r := range jc.result.RecordsWithDiff {
		for name := range columnsToDelete {
			delete(r, name)
		}
	}
}
