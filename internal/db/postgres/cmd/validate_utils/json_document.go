package validate_utils

import (
	"encoding/json"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump_objects"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type Documenter interface {
	Append(original, transformed *pgcopy.Row) error
	Data() ([]byte, error)
}

type values struct {
	Original    string `json:"original,omitempty"`
	Transformed string `json:"transformed,omitempty"`
	Changed     bool   `json:"changed,omitempty"`
	Implicit    bool   `json:"implicit,omitempty"`
}

type jsonDocument struct {
	Schema            string       `json:"schema"`
	Name              string       `json:"name"`
	PrimaryKeyColumns []string     `json:"primary_key_columns,omitempty"`
	Records           []jsonRecord `json:"records,omitempty"`
}

type jsonRecord map[string]*values

type JsonDocument struct {
	result                  *jsonDocument
	table                   *dump_objects.Table
	withDiff                bool
	expectedAffectedColumns map[int]struct{}
	pkColumns               map[int]*toolkit.Column
}

func NewJsonDocument(table *dump_objects.Table, withDiff bool) *JsonDocument {
	pkColumns := getPrimaryKeyConstraintColumns(table)
	expectedAffectedColumns := getAffectedColumns(table)
	var pkColumnsList []string
	for _, c := range pkColumns {
		pkColumnsList = append(pkColumnsList, c.Name)
	}

	return &JsonDocument{
		result: &jsonDocument{
			PrimaryKeyColumns: pkColumnsList,
			Records:           make([]jsonRecord, 0),
		},
		withDiff:                withDiff,
		table:                   table,
		pkColumns:               pkColumns,
		expectedAffectedColumns: expectedAffectedColumns,
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

		r[c.Name] = &values{
			Original:    getStringFromRawValue(originalRawValue),
			Transformed: getStringFromRawValue(transformedRawValue),
			Changed:     ValuesEqual(originalRawValue, transformedRawValue),
		}
		jc.result.Records = append(jc.result.Records)
	}
	return nil
}

func (jc *JsonDocument) Data() ([]byte, error) {
	return json.Marshal(jc.result)
}
