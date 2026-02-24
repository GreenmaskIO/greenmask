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
	table := edge.from.table
	for _, key := range edge.from.keys {
		leftTableKeys = append(leftTableKeys, fmt.Sprintf(`%s__%s__%s`, table.Schema, table.Name, key.Name))
	}

	rightTable := edge.to
	for idx := 0; idx < len(edge.to.keys); idx++ {

		leftPart := fmt.Sprintf(
			`"%s"."%s"`,
			initTableName,
			leftTableKeys[idx],
		)

		rightPart := edge.to.keys[idx].GetKeyReference(rightTable.table)
		conds = append(conds, fmt.Sprintf(`%s = %s`, leftPart, rightPart))
	}
	if len(edge.from.polymorphicExprs) > 0 {
		conds = append(conds, edge.from.polymorphicExprs...)
	}
	if len(edge.to.polymorphicExprs) > 0 {
		conds = append(conds, edge.to.polymorphicExprs...)
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

		leftPart := edge.from.keys[idx].GetKeyReference(leftTable)
		rightPart := edge.to.keys[idx].GetKeyReference(rightTable)

		if override, ok := overriddenTables[rightTable.Oid]; ok {
			rightPart = fmt.Sprintf(
				`"%s"."%s"`,
				override,
				edge.to.keys[idx].Name,
			)
		}

		conds = append(conds, fmt.Sprintf(`%s = %s`, leftPart, rightPart))
		if len(edge.to.table.SubsetConds) > 0 {
			conds = append(conds, edge.to.table.SubsetConds...)
		}
	}

	if len(edge.from.polymorphicExprs) > 0 {
		conds = append(conds, edge.from.polymorphicExprs...)
	}
	if len(edge.to.polymorphicExprs) > 0 {
		conds = append(conds, edge.to.polymorphicExprs...)
	}

	rightTableName := fmt.Sprintf(`"%s"."%s"`, rightTable.Schema, rightTable.Name)
	if override, ok := overriddenTables[rightTable.Oid]; ok {
		rightTableName = override
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
