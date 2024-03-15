package validate_utils

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type Documenter interface {
	Print(w io.Writer) error
	Append(original, transformed *pgcopy.Row) error
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

type jsonRecordWithDiff map[string]*valueWithDiff

type jsonRecordPlain map[string]string

type JsonDocument struct {
	result                    *JsonDocumentResult
	table                     *entries.Table
	withDiff                  bool
	expectedAffectedColumns   map[string]struct{}
	unexpectedAffectedColumns map[string]struct{}
	pkColumns                 map[int]*toolkit.Column
	onlyTransformed           bool
}

func NewJsonDocument(table *entries.Table, withDiff bool, onlyTransformed bool) *JsonDocument {
	pkColumns := getPrimaryKeyConstraintColumns(table)
	expectedAffectedColumns := getAffectedColumns(table)
	var pkColumnsList []string
	for _, c := range pkColumns {
		pkColumnsList = append(pkColumnsList, c.Name)
	}

	return &JsonDocument{
		result: &JsonDocumentResult{
			Schema:            table.Schema,
			Name:              table.Name,
			PrimaryKeyColumns: pkColumnsList,
			WithDiff:          withDiff,
			OnlyTransformed:   onlyTransformed,
			RecordsWithDiff:   make([]jsonRecordWithDiff, 0),
		},
		withDiff:                  withDiff,
		table:                     table,
		pkColumns:                 pkColumns,
		expectedAffectedColumns:   expectedAffectedColumns,
		unexpectedAffectedColumns: make(map[string]struct{}),
		onlyTransformed:           onlyTransformed,
	}
}

func (jc *JsonDocument) Append(original, transformed *pgcopy.Row) error {
	r := make(jsonRecordWithDiff)
	for idx, c := range jc.table.Columns {
		originalRawValue, err := original.GetColumn(idx)
		if err != nil {
			return fmt.Errorf("error getting column from original record: %w", err)
		}

		var originalValue, transformedValue string

		originalValue = getStringFromRawValue(originalRawValue)

		equal := true
		expected := true

		transformedRawValue, err := transformed.GetColumn(idx)
		if err != nil {
			return fmt.Errorf("error getting column from transformed record: %w", err)
		}
		transformedValue = getStringFromRawValue(transformedRawValue)
		equal = ValuesEqual(originalRawValue, transformedRawValue)
		if _, ok := jc.expectedAffectedColumns[c.Name]; !equal && !ok {
			expected = false
			jc.unexpectedAffectedColumns[c.Name] = struct{}{}
		}

		r[c.Name] = &valueWithDiff{
			Original:    originalValue,
			Transformed: transformedValue,
			Equal:       equal,
			Expected:    expected,
			ColNum:      idx,
		}
	}
	jc.result.RecordsWithDiff = append(jc.result.RecordsWithDiff, r)
	return nil
}

func (jc *JsonDocument) Print(w io.Writer) error {
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
		if err := json.NewEncoder(w).Encode(response); err != nil {
			return err
		}
		return nil
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
	if err := json.NewEncoder(w).Encode(response); err != nil {
		return err
	}
	return nil
}

func (jc *JsonDocument) GetUnexpectedlyChangedColumns() map[string]struct{} {
	return jc.unexpectedAffectedColumns
}

func (jc *JsonDocument) GetAffectedColumns() map[string]struct{} {
	affectedColumns := maps.Clone(jc.expectedAffectedColumns)
	maps.Copy(affectedColumns, jc.unexpectedAffectedColumns)
	return affectedColumns
}

func (jc *JsonDocument) GetColumnsToPrint() map[string]struct{} {
	if jc.onlyTransformed {
		columnsToPrint := jc.GetAffectedColumns()
		for _, colName := range jc.result.PrimaryKeyColumns {
			columnsToPrint[colName] = struct{}{}
		}
		return columnsToPrint
	}

	columnsToPrint := make(map[string]struct{}, len(jc.table.Columns))
	for _, c := range jc.table.Columns {
		columnsToPrint[c.Name] = struct{}{}
	}
	return columnsToPrint
}

func (jc *JsonDocument) Get() *JsonDocumentResult {
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
		if _, ok := columnsToPrint[c.Name]; !ok {
			columnsToDelete[c.Name] = struct{}{}
		}
	}

	for _, r := range jc.result.RecordsWithDiff {
		for name := range columnsToDelete {
			delete(r, name)
		}
	}
}
