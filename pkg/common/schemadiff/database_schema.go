package schemadiff

import (
	"fmt"
	"slices"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

func newDiffNode(event string, signature map[string]string) core.DiffNode {
	return core.DiffNode{
		Event:     event,
		Msg:       core.DiffEventMsgs[event],
		Signature: signature,
	}
}

type DatabaseSchema []core.Table

func (ds DatabaseSchema) Diff(current DatabaseSchema) []core.DiffNode {
	var res []core.DiffNode
	for _, currentState := range current {
		previousState, ok := ds.getTableByID(currentState.ID)
		if !ok {
			// if the table was not found by ID it was likely re-created
			previousState, ok = ds.getTableByName(currentState.Schema, currentState.Name)
		}

		if !ok {
			res = append(res, newDiffNode(core.TableCreatedDiffEvent, map[string]string{
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

func (ds DatabaseSchema) getTableByID(id int) (core.Table, bool) {
	idx := slices.IndexFunc(ds, func(table core.Table) bool {
		return table.ID == id
	})
	if idx == -1 {
		return core.Table{}, false
	}
	return ds[idx], true
}

func (ds DatabaseSchema) getTableByName(schemaName, tableName string) (core.Table, bool) {
	idx := slices.IndexFunc(ds, func(table core.Table) bool {
		return table.Schema == schemaName && table.Name == tableName
	})
	if idx == -1 {
		return core.Table{}, false
	}
	return ds[idx], true
}

func diffTables(previous, current core.Table) []core.DiffNode {
	var res []core.DiffNode

	if previous.Schema != current.Schema {
		res = append(res, newDiffNode(core.TableMovedToAnotherSchemaDiffEvent, map[string]string{
			"PreviousSchemaName": previous.Schema,
			"CurrentSchemaName":  current.Schema,
			"TableName":          current.Name,
			"TableID":            fmt.Sprintf("%d", previous.ID),
		}))
	}

	if previous.Name != current.Name {
		res = append(res, newDiffNode(core.TableRenamedDiffEvent, map[string]string{
			"PreviousTableName": previous.Name,
			"CurrentTableName":  current.Name,
			"SchemaName":        current.Schema,
			"TableID":           fmt.Sprintf("%d", previous.ID),
		}))
	}

	res = append(res, diffTableColumns(previous, current)...)

	return res
}

func diffTableColumns(previous, current core.Table) []core.DiffNode {
	var res []core.DiffNode
	for _, currentStateColumn := range current.Columns {
		previousStateColumn, ok := findColumnByIdx(previous, currentStateColumn.Idx)
		if !ok {
			previousStateColumn, ok = findColumnByName(previous, currentStateColumn.Name)
		}

		if !ok {
			res = append(res, newDiffNode(core.ColumnCreatedDiffEvent, map[string]string{
				"TableSchema": previous.Schema,
				"TableName":   previous.Name,
				"ColumnName":  currentStateColumn.Name,
				// TODO: Replace it with type def such as NUMERIC(10, 2) VARCHAR(128), etc.
				"ColumnType": currentStateColumn.TypeName,
			}))
			continue
		}

		if currentStateColumn.Name != previousStateColumn.Name {
			res = append(res, newDiffNode(core.ColumnRenamedDiffEvent, map[string]string{
				"TableSchema":        previous.Schema,
				"TableName":          previous.Name,
				"PreviousColumnName": previousStateColumn.Name,
				"CurrentColumnName":  currentStateColumn.Name,
			}))
		}

		if currentStateColumn.TypeID != previousStateColumn.TypeID {
			res = append(res, newDiffNode(core.ColumnTypeChangedDiffEvent, map[string]string{
				"TableSchema":          previous.Schema,
				"TableName":            previous.Name,
				"ColumnName":           previousStateColumn.Name,
				"PreviousColumnType":   previousStateColumn.TypeName,
				"PreviousColumnTypeID": fmt.Sprintf("%d", previousStateColumn.TypeID),
				"CurrentColumnType":    currentStateColumn.TypeName,
				"CurrentColumnTypeID":  fmt.Sprintf("%d", currentStateColumn.TypeID),
			}))
		}
	}
	return res
}

func findColumnByIdx(t core.Table, idx int) (core.Column, bool) {
	i := slices.IndexFunc(t.Columns, func(column core.Column) bool {
		return column.Idx == idx
	})
	if i == -1 {
		return core.Column{}, false
	}
	return t.Columns[i], true
}

func findColumnByName(t core.Table, name string) (core.Column, bool) {
	i := slices.IndexFunc(t.Columns, func(column core.Column) bool {
		return column.Name == name
	})
	if i == -1 {
		return core.Column{}, false
	}
	return t.Columns[i], true
}
