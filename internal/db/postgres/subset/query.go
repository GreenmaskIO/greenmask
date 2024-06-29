package subset

import (
	"fmt"
	"slices"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/rs/zerolog/log"
)

func generateAndSetQuery(path *Path, tables []*entries.Table) {
	// We start DFS from the root scope
	table := tables[path.RootVertex]
	if table.Name == "businessentity" {
		log.Debug()
	}
	query := generateQueriesDfs(path, tables, rootScopeId, false)
	fmt.Printf("%s.%s\n", table.Schema, table.Name)
	fmt.Println(query)
	table.Query = query
}

func generateQueriesDfs(path *Path, tables []*entries.Table, scopeId int, isSubQuery bool) string {

	if len(path.ScopeEdges[scopeId]) == 0 && isSubQuery {
		return ""
	}
	currentScopeQuery := generateQuery(tables, path.RootVertex, path.ScopeEdges[scopeId], isSubQuery)
	var subQueries []string
	for _, nextScopeId := range path.ScopeGraph[scopeId] {
		subQuery := generateQueriesDfs(path, tables, nextScopeId, true)
		if subQuery != "" {
			subQueries = append(subQueries, subQuery)
		}
	}

	if len(subQueries) == 0 {
		return currentScopeQuery
	}

	totalQuery := fmt.Sprintf(
		"%s AND %s", currentScopeQuery,
		strings.Join(subQueries, " AND "),
	)
	return totalQuery
}

// TODO: Start always WHERE TRUE AND ...
func generateQuery(tables []*entries.Table, rootTableIdx int, edges []*Edge, isSubQuery bool) string {

	// Use root table as a root table from path
	rootTable := tables[rootTableIdx]
	var leftTableEdge *Edge
	if isSubQuery {
		// If it is not a root scope use the right table from the first edge as a root table
		// And left table from the first edge as a left table for the subquery. It will be used for where in clause
		leftTableEdge = edges[0]
		rootTable = tables[edges[0].B.Idx]
		edges = edges[1:]
	}

	subsetConds := slices.Clone(rootTable.SubsetConds)
	selectClause := fmt.Sprintf(`SELECT "%s"."%s".*`, rootTable.Schema, rootTable.Name)
	if isSubQuery {
		selectClause = generateSelectByPrimaryKey(rootTable)
	}
	fromClause := fmt.Sprintf(`FROM "%s"."%s" `, rootTable.Schema, rootTable.Name)

	var joinClauses []string
	for _, e := range edges {
		rightTable := e.B
		if len(rightTable.Table.SubsetConds) > 0 {
			subsetConds = append(subsetConds, rightTable.Table.SubsetConds...)
		}
		joinClause := generateJoinClause(e)
		joinClauses = append(joinClauses, joinClause)
	}

	query := fmt.Sprintf(
		`%s %s %s %s`,
		selectClause,
		fromClause,
		strings.Join(joinClauses, " "),
		generateWhereClause(subsetConds),
	)

	if isSubQuery {
		if leftTableEdge == nil {
			panic("leftTableEdge is nil")
		}
		var leftTableConds []string
		for _, k := range leftTableEdge.A.Keys {
			leftTableConds = append(leftTableConds, fmt.Sprintf(`"%s"."%s"."%s"`, leftTableEdge.A.Table.Schema, leftTableEdge.A.Table.Name, k))
		}
		query = fmt.Sprintf("((%s) IN (%s))", strings.Join(leftTableConds, ", "), query)
	}

	return query
}

func generateJoinClause(edge *Edge) string {
	var conds []string
	leftTable, rightTable := edge.A, edge.B
	for idx := 0; idx < len(leftTable.Keys); idx++ {

		leftPart := fmt.Sprintf(
			`"%s"."%s"."%s"`,
			leftTable.Table.Schema,
			leftTable.Table.Name,
			leftTable.Keys[idx],
		)

		rightPart := fmt.Sprintf(
			`"%s"."%s"."%s"`,
			rightTable.Table.Schema,
			rightTable.Table.Name,
			rightTable.Keys[idx],
		)

		conds = append(conds, fmt.Sprintf(`%s = %s`, leftPart, rightPart))
	}

	rightTableName := fmt.Sprintf(`"%s"."%s"`, rightTable.Table.Schema, rightTable.Table.Name)

	joinClause := fmt.Sprintf(
		`JOIN %s ON %s`,
		rightTableName,
		strings.Join(conds, " AND "),
	)
	return joinClause
}

func generateWhereClause(subsetConds []string) string {
	if len(subsetConds) == 0 {
		return ""
	}
	escapedConds := make([]string, 0, len(subsetConds))
	for _, cond := range subsetConds {
		escapedConds = append(escapedConds, fmt.Sprintf(`( %s )`, cond))
	}
	return "WHERE " + strings.Join(escapedConds, " AND ")
}

func generateSelectByPrimaryKey(table *entries.Table) string {
	var keys []string
	for _, key := range table.PrimaryKey {
		keys = append(keys, fmt.Sprintf(`"%s"."%s"."%s"`, table.Schema, table.Name, key))
	}
	return fmt.Sprintf(
		`SELECT %s`,
		strings.Join(keys, ", "),
	)
}

func generateSelectDistinctByPrimaryKey(table *entries.Table) string {
	var keys []string
	for _, key := range table.PrimaryKey {
		keys = append(keys, fmt.Sprintf(`"%s"."%s"."%s"`, table.Schema, table.Name, key))
	}
	return fmt.Sprintf(
		`SELECT DISTINCT ON (%s) "%s"."%s".*`,
		strings.Join(keys, ", "),
		table.Schema,
		table.Name,
	)
}

func generateSelectDistinctWithCast(table *entries.Table) string {
	var columns []string
	for _, c := range table.Columns {
		columns = append(columns, fmt.Sprintf(`CAST("%s"."%s"."%s" AS text)`, table.Schema, table.Name, c.Name))
	}
	return fmt.Sprintf(`SELECT DISTINCT %s`, strings.Join(columns, ", "))
}
