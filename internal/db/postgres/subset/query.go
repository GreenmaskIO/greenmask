package subset

import (
	"fmt"
	"slices"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
)

func findSubsetVertexes(graph [][]*Edge, tables []*entries.Table) map[int][]int {
	subsetVertexes := make(map[int][]int)
	for v := range graph {
		var path []int
		if len(tables[v].SubsetConds) > 0 {
			path = []int{v}
		}
		stack := []int{v}
		subsetDfs(tables, graph, v, &path, &stack)
		if len(path) > 0 {
			subsetVertexes[v] = TopologicalSort(graph, path)
		}
	}
	return subsetVertexes
}

func subsetDfs(tables []*entries.Table, graph [][]*Edge, v int, path *[]int, stack *[]int) {

	for _, to := range graph[v] {
		*stack = append(*stack, to.Idx)
		if len(tables[to.Idx].SubsetConds) > 0 {
			for _, s := range *stack {
				if !slices.Contains(*path, s) {
					*path = append(*path, s)
				}
			}
		}
		subsetDfs(tables, graph, to.Idx, path, stack)
		*stack = (*stack)[:len(*stack)-1]
	}
}

func setQueriesV2(path []int, tableIdx int, graph [][]*Edge, tables []*entries.Table) {
	rootIdx := path[0]
	rootTable := tables[rootIdx]
	subsetConds := slices.Clone(rootTable.SubsetConds)
	var joinClauses []string
	prevIdx := rootIdx

	fromClause := fmt.Sprintf(`FROM "%s"."%s" `, rootTable.Schema, rootTable.Name)
	for _, v := range path[1:] {
		table := tables[v]
		if len(table.SubsetConds) > 0 {
			subsetConds = append(subsetConds, table.SubsetConds...)
		}
		var edges []*Edge
		for _, e := range graph[v] {
			l, r := e.GetLeftAndRightTable(prevIdx)
			if slices.Contains(path, r.Idx) && slices.Contains(path, l.Idx) {
				edges = append(edges, e)
			}
		}
		joinClause := generateJoinClauseV3(edges, prevIdx)
		joinClauses = append(joinClauses, joinClause)
		prevIdx = v
	}

	table := tables[tableIdx]

	var selectClause string
	if len(table.PrimaryKey) > 0 {
		selectClause = generateSelectDistinctByPrimaryKey(table)
	} else {
		selectClause = generateSelectDistinctWithCast(table)
	}
	query := fmt.Sprintf(
		`%s %s %s %s`,
		selectClause,
		fromClause,
		strings.Join(joinClauses, " "),
		generateWhereClause(subsetConds),
	)
	table.Query = query
}

func generateJoinClauseV3(edges []*Edge, previousTableIdx int) string {
	var conds []string
	for _, e := range edges {
		leftTable, rightTable := e.A, e.B
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
	}

	_, rightTable := edges[0].GetLeftAndRightTable(previousTableIdx)
	rightTableName := fmt.Sprintf(`"%s"."%s"`, rightTable.Table.Schema, rightTable.Table.Name)

	joinClause := fmt.Sprintf(
		`JOIN %s ON %s`,
		rightTableName,
		strings.Join(conds, " AND "),
	)
	return joinClause
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
