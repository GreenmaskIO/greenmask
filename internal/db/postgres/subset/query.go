package subset

import (
	"fmt"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	joinTypeInner = "INNER"
	joinTypeLeft  = "LEFT"
)

func generateJoinClauseForDroppedEdge(edge *Edge, initTableName string) string {
	var conds []string

	var leftTableKeys []string
	keys := edge.from.keys
	table := edge.from.table
	for _, key := range keys {
		leftTableKeys = append(leftTableKeys, fmt.Sprintf(`%s__%s__%s`, table.Schema, table.Name, key))
	}

	rightTable := edge.to
	for idx := 0; idx < len(edge.to.keys); idx++ {

		leftPart := fmt.Sprintf(
			`"%s"."%s"`,
			initTableName,
			leftTableKeys[idx],
		)

		rightPart := fmt.Sprintf(
			`"%s"."%s"."%s"`,
			rightTable.table.Schema,
			rightTable.table.Name,
			edge.to.keys[idx],
		)

		conds = append(conds, fmt.Sprintf(`%s = %s`, leftPart, rightPart))
	}

	rightTableName := fmt.Sprintf(`"%s"."%s"`, edge.to.table.Schema, edge.to.table.Name)

	joinClause := fmt.Sprintf(
		`JOIN %s ON %s`,
		rightTableName,
		strings.Join(conds, " AND "),
	)
	return joinClause
}

func generateJoinClauseV2(edge *Edge, joinType string, overriddenTables map[toolkit.Oid]string) string {
	if joinType != joinTypeInner && joinType != joinTypeLeft {
		panic(fmt.Sprintf("invalid join type: %s", joinType))
	}

	var conds []string

	leftTable, rightTable := edge.from.table, edge.to.table
	for idx := 0; idx < len(edge.from.keys); idx++ {

		leftPart := fmt.Sprintf(
			`"%s"."%s"."%s"`,
			leftTable.Table.Schema,
			leftTable.Table.Name,
			edge.from.keys[idx],
		)

		rightPart := fmt.Sprintf(
			`"%s"."%s"."%s"`,
			rightTable.Table.Schema,
			rightTable.Table.Name,
			edge.to.keys[idx],
		)
		if override, ok := overriddenTables[rightTable.Table.Oid]; ok {
			rightPart = fmt.Sprintf(
				`"%s"."%s"`,
				override,
				edge.to.keys[idx],
			)
		}

		conds = append(conds, fmt.Sprintf(`%s = %s`, leftPart, rightPart))
		if len(edge.to.table.SubsetConds) > 0 {
			conds = append(conds, edge.to.table.SubsetConds...)
		}
	}

	rightTableName := fmt.Sprintf(`"%s"."%s"`, rightTable.Table.Schema, rightTable.Table.Name)
	if override, ok := overriddenTables[rightTable.Table.Oid]; ok {
		rightTableName = fmt.Sprintf(`"%s"`, override)
	}

	joinClause := fmt.Sprintf(
		`%s JOIN %s ON %s`,
		joinType,
		rightTableName,
		strings.Join(conds, " AND "),
	)
	return joinClause
}

func generateWhereClause(subsetConds []string) string {
	if len(subsetConds) == 0 {
		return "WHERE TRUE"
	}
	escapedConds := make([]string, 0, len(subsetConds))
	for _, cond := range subsetConds {
		escapedConds = append(escapedConds, fmt.Sprintf(`( %s )`, cond))
	}
	return "WHERE " + strings.Join(escapedConds, " AND ")
}

func generateSelectByPrimaryKey(table *entries.Table, pk []string) string {
	var keys []string
	for _, key := range pk {
		keys = append(keys, fmt.Sprintf(`"%s"."%s"."%s"`, table.Schema, table.Name, key))
	}
	return fmt.Sprintf(
		`SELECT %s`,
		strings.Join(keys, ", "),
	)
}
