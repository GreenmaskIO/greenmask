package validate_utils

import (
	"encoding/json"
	"fmt"
	"maps"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump_objects"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type values struct {
	ColNum      int    `json:"-"`
	Original    string `json:"original,omitempty"`
	Transformed string `json:"transformed,omitempty"`
	Equal       bool   `json:"equal,omitempty"`
	Expected    bool   `json:"implicit,omitempty"`
}

type JsonDocumentResult struct {
	Schema            string       `json:"schema"`
	Name              string       `json:"name"`
	PrimaryKeyColumns []string     `json:"primary_key_columns,omitempty"`
	Records           []jsonRecord `json:"records,omitempty"`
}

type jsonRecord map[string]*values

type JsonDocument struct {
	result                    *JsonDocumentResult
	table                     *dump_objects.Table
	withDiff                  bool
	expectedAffectedColumns   map[string]struct{}
	unexpectedAffectedColumns map[string]struct{}
	pkColumns                 map[int]*toolkit.Column
	onlyTransformed           bool
}

func NewJsonDocument(table *dump_objects.Table, withDiff bool, onlyTransformed bool) *JsonDocument {
	pkColumns := getPrimaryKeyConstraintColumns(table)
	expectedAffectedColumns := getAffectedColumns(table)
	var pkColumnsList []string
	for _, c := range pkColumns {
		pkColumnsList = append(pkColumnsList, c.Name)
	}

	return &JsonDocument{
		result: &JsonDocumentResult{
			PrimaryKeyColumns: pkColumnsList,
			Records:           make([]jsonRecord, 0),
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
	r := make(jsonRecord)
	for idx, c := range jc.table.Columns {
		originalRawValue, err := original.GetColumn(idx)
		if err != nil {
			return fmt.Errorf("error getting column from original record: %w", err)
		}
		transformedRawValue, err := transformed.GetColumn(idx)
		if err != nil {
			return fmt.Errorf("error getting column from transformed record: %w", err)
		}

		equal := ValuesEqual(originalRawValue, transformedRawValue)
		expected := true
		if _, ok := jc.expectedAffectedColumns[c.Name]; !equal && !ok {
			expected = false
			jc.unexpectedAffectedColumns[c.Name] = struct{}{}
		}

		r[c.Name] = &values{
			Original:    getStringFromRawValue(originalRawValue),
			Transformed: getStringFromRawValue(transformedRawValue),
			Equal:       equal,
			Expected:    expected,
			ColNum:      idx,
		}
	}
	jc.result.Records = append(jc.result.Records, r)
	return nil
}

func (jc *JsonDocument) GetImplicitlyChangedColumns() map[string]struct{} {
	return jc.unexpectedAffectedColumns
}

func (jc *JsonDocument) GetColumnsToPrint() map[string]struct{} {
	if jc.onlyTransformed {
		columnsToPrint := maps.Clone(jc.expectedAffectedColumns)
		maps.Copy(columnsToPrint, jc.unexpectedAffectedColumns)
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

func (jc *JsonDocument) Marshal() ([]byte, error) {
	// TODO:
	//	 1. Return all columns if requested
	//   2. Return only transformed data.
	//  	2.1 Analyze affected columns + unexpectedly affected
	// 		2.2 Return all affected columns with data + primary key
	if jc.onlyTransformed {
		jc.filterColumns()
	}
	return json.Marshal(jc.result)
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

	for _, r := range jc.result.Records {
		for name := range columnsToDelete {
			delete(r, name)
		}
	}
}
