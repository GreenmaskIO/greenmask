package toolkit

import (
	"fmt"
	"slices"

	"github.com/rs/zerolog/log"
)

const (
	TableMovedToAnotherSchemaDiffEvent = "TableMovedToAnotherSchema"
	TableRenamedDiffEvent              = "TableRenamed"
	TableCreatedDiffEvent              = "TableCreated"
	ColumnCreatedDiffEvent             = "ColumnCreated"
	ColumnRenamedDiffEvent             = "ColumnRenamed"
	ColumnTypeChangedDiffEvent         = "ColumnTypeChanged"
)

var DiffEventMsgs = map[string]string{
	TableMovedToAnotherSchemaDiffEvent: "Table moved to another schema",
	TableRenamedDiffEvent:              "Table renamed",
	TableCreatedDiffEvent:              "Table created",
	ColumnCreatedDiffEvent:             "Column created",
	ColumnRenamedDiffEvent:             "Column renamed",
	ColumnTypeChangedDiffEvent:         "Column type changed",
}

type DiffNode struct {
	Event     string            `json:"event,omitempty"`
	Signature map[string]string `json:"signature,omitempty"`
}

type DatabaseSchema []*Table

func (ds DatabaseSchema) Diff(current DatabaseSchema) (res []*DiffNode) {

	for _, currentState := range current {
		if currentState.Kind == "r" && currentState.Parent != 0 {
			continue
		}

		previousState, ok := ds.getTableByOid(currentState.Oid)
		if !ok {
			// if the table was not found by oid it was likely re-created
			// Consider: Should we notify about table re-creation?
			previousState, ok = ds.getTableByName(currentState.Schema, currentState.Name)
		}

		if currentState.Name == "test" {
			log.Debug()
		}

		if !ok {
			signature := map[string]string{
				"SchemaName": currentState.Schema,
				"TableName":  currentState.Name,
				"TableOid":   fmt.Sprintf("%d", currentState.Oid),
			}
			res = append(res, &DiffNode{Event: TableCreatedDiffEvent, Signature: signature})
			continue
		}

		res = append(res, diffTables(previousState, currentState)...)

	}
	return res
}

func (ds DatabaseSchema) getTableByOid(oid Oid) (*Table, bool) {
	idx := slices.IndexFunc(ds, func(table *Table) bool {
		return table.Oid == oid
	})
	if idx == -1 {
		return nil, false
	}
	return ds[idx], true
}

func (ds DatabaseSchema) getTableByName(schemaName, tableName string) (*Table, bool) {
	idx := slices.IndexFunc(ds, func(table *Table) bool {
		return table.Schema == schemaName && table.Name == tableName
	})
	if idx == -1 {
		return nil, false
	}
	return ds[idx], true
}

func diffTables(previous, current *Table) (res []*DiffNode) {
	if previous.Schema != current.Schema {
		node := &DiffNode{
			Event: TableMovedToAnotherSchemaDiffEvent,

			Signature: map[string]string{
				"PreviousSchemaName": previous.Schema,
				"CurrentSchemaName":  current.Schema,
				"TableName":          current.Name,
				"TableOid":           fmt.Sprintf("%d", previous.Oid),
			},
		}
		res = append(res, node)
	}

	if previous.Name != current.Name {
		node := &DiffNode{
			Event: TableRenamedDiffEvent,

			Signature: map[string]string{
				"PreviousTableName": previous.Name,
				"CurrentTableName":  current.Name,
				"SchemaName":        current.Schema,
				"TableOid":          fmt.Sprintf("%d", previous.Oid),
			},
		}
		res = append(res, node)
	}

	res = append(res, diffTableColumns(previous, current)...)

	return
}

func diffTableColumns(previous, current *Table) (res []*DiffNode) {
	for _, currentStateColumn := range current.Columns {

		previousStateColumn, ok := findColumnByAttNum(previous, currentStateColumn.Num)
		if !ok {
			previousStateColumn, ok = findColumnByName(previous, currentStateColumn.Name)
		}

		if !ok {
			node := &DiffNode{
				Event: ColumnCreatedDiffEvent,

				Signature: map[string]string{
					"TableSchema": previous.Schema,
					"TableName":   previous.Name,
					"ColumnName":  currentStateColumn.Name,
					// TODO: Replace it with type def such as NUMERIC(10, 2) VARCHAR(128), etc.
					"ColumnType": currentStateColumn.TypeName,
				},
			}
			res = append(res, node)
			continue
		}

		if currentStateColumn.Name != previousStateColumn.Name {
			node := &DiffNode{
				Event: ColumnRenamedDiffEvent,

				Signature: map[string]string{
					"TableSchema":        previous.Schema,
					"TableName":          previous.Name,
					"PreviousColumnName": previousStateColumn.Name,
					"CurrentColumnName":  currentStateColumn.Name,
				},
			}
			res = append(res, node)
		}

		if currentStateColumn.TypeOid != previousStateColumn.TypeOid {
			node := &DiffNode{
				Event: ColumnTypeChangedDiffEvent,

				Signature: map[string]string{
					"TableSchema":           previous.Schema,
					"TableName":             previous.Name,
					"ColumnName":            previousStateColumn.Name,
					"PreviousColumnType":    previousStateColumn.TypeName,
					"PreviousColumnTypeOid": fmt.Sprintf("%d", previousStateColumn.TypeOid),
					"CurrentColumnType":     currentStateColumn.TypeName,
					"CurrentColumnTypeOid":  fmt.Sprintf("%d", currentStateColumn.TypeOid),
				},
			}
			res = append(res, node)
		}
	}
	return
}

func findColumnByAttNum(t *Table, num AttNum) (*Column, bool) {
	idx := slices.IndexFunc(t.Columns, func(column *Column) bool {
		return column.Num == num
	})
	if idx == -1 {
		return nil, false
	}
	return t.Columns[idx], true
}

func findColumnByName(t *Table, name string) (*Column, bool) {
	idx := slices.IndexFunc(t.Columns, func(column *Column) bool {
		return column.Name == name
	})
	if idx == -1 {
		return nil, false
	}
	return t.Columns[idx], true
}
