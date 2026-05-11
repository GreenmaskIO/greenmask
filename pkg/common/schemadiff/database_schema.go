package schemadiff

import (
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/pkg/common/models"
)

func newDiffNode(event string, signature map[string]string) models.DiffNode {
	return models.DiffNode{
		Event:     event,
		Msg:       models.DiffEventMsgs[event],
		Signature: signature,
	}
}

type DatabaseSchema []models.Table

func (ds DatabaseSchema) Diff(current DatabaseSchema) []models.DiffNode {
	var res []models.DiffNode
	for _, currentState := range current {
		previousState, ok := ds.getTableByID(currentState.ID)
		if !ok {
			// if the table was not found by ID it was likely re-created
			previousState, ok = ds.getTableByName(currentState.Schema, currentState.Name)
		}

		if !ok {
			res = append(res, newDiffNode(models.TableCreatedDiffEvent, map[string]string{
				"SchemaName": currentState.Schema,
				"TableName":  currentState.Name,
				"TableID":    fmt.Sprintf("%d", currentState.ID),
			}))
			continue
		}

		res = append(res, diffTables(previousState, currentState)...)
	}
	return res
}

func (ds DatabaseSchema) getTableByID(id int) (models.Table, bool) {
	idx := slices.IndexFunc(ds, func(table models.Table) bool {
		return table.ID == id
	})
	if idx == -1 {
		return models.Table{}, false
	}
	return ds[idx], true
}

func (ds DatabaseSchema) getTableByName(schemaName, tableName string) (models.Table, bool) {
	idx := slices.IndexFunc(ds, func(table models.Table) bool {
		return table.Schema == schemaName && table.Name == tableName
	})
	if idx == -1 {
		return models.Table{}, false
	}
	return ds[idx], true
}

func diffTables(previous, current models.Table) []models.DiffNode {
	var res []models.DiffNode

	if previous.Schema != current.Schema {
		res = append(res, newDiffNode(models.TableMovedToAnotherSchemaDiffEvent, map[string]string{
			"PreviousSchemaName": previous.Schema,
			"CurrentSchemaName":  current.Schema,
			"TableName":          current.Name,
			"TableID":            fmt.Sprintf("%d", previous.ID),
		}))
	}

	if previous.Name != current.Name {
		res = append(res, newDiffNode(models.TableRenamedDiffEvent, map[string]string{
			"PreviousTableName": previous.Name,
			"CurrentTableName":  current.Name,
			"SchemaName":        current.Schema,
			"TableID":           fmt.Sprintf("%d", previous.ID),
		}))
	}

	res = append(res, diffTableColumns(previous, current)...)

	return res
}

func diffTableColumns(previous, current models.Table) []models.DiffNode {
	var res []models.DiffNode
	for _, currentStateColumn := range current.Columns {
		previousStateColumn, ok := findColumnByIdx(previous, currentStateColumn.Idx)
		if !ok {
			previousStateColumn, ok = findColumnByName(previous, currentStateColumn.Name)
		}

		if !ok {
			res = append(res, newDiffNode(models.ColumnCreatedDiffEvent, map[string]string{
				"TableSchema": previous.Schema,
				"TableName":   previous.Name,
				"ColumnName":  currentStateColumn.Name,
				// TODO: Replace it with type def such as NUMERIC(10, 2) VARCHAR(128), etc.
				"ColumnType": currentStateColumn.TypeName,
			}))
			continue
		}

		if currentStateColumn.Name != previousStateColumn.Name {
			res = append(res, newDiffNode(models.ColumnRenamedDiffEvent, map[string]string{
				"TableSchema":        previous.Schema,
				"TableName":          previous.Name,
				"PreviousColumnName": previousStateColumn.Name,
				"CurrentColumnName":  currentStateColumn.Name,
			}))
		}

		if currentStateColumn.TypeOID != previousStateColumn.TypeOID {
			res = append(res, newDiffNode(models.ColumnTypeChangedDiffEvent, map[string]string{
				"TableSchema":           previous.Schema,
				"TableName":             previous.Name,
				"ColumnName":            previousStateColumn.Name,
				"PreviousColumnType":    previousStateColumn.TypeName,
				"PreviousColumnTypeOID": fmt.Sprintf("%d", previousStateColumn.TypeOID),
				"CurrentColumnType":     currentStateColumn.TypeName,
				"CurrentColumnTypeOID":  fmt.Sprintf("%d", currentStateColumn.TypeOID),
			}))
		}
	}
	return res
}

func findColumnByIdx(t models.Table, idx int) (models.Column, bool) {
	i := slices.IndexFunc(t.Columns, func(column models.Column) bool {
		return column.Idx == idx
	})
	if i == -1 {
		return models.Column{}, false
	}
	return t.Columns[i], true
}

func findColumnByName(t models.Table, name string) (models.Column, bool) {
	i := slices.IndexFunc(t.Columns, func(column models.Column) bool {
		return column.Name == name
	})
	if i == -1 {
		return models.Column{}, false
	}
	return t.Columns[i], true
}
