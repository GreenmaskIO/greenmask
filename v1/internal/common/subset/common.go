// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package subset

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/huandu/go-sqlbuilder"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/condensationgraph"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

// Dialect - represents the SQL dialect used for building queries.
type Dialect int

const (
	DialectPostgres = Dialect(sqlbuilder.PostgreSQL)
	DialectMySQL    = Dialect(sqlbuilder.MySQL)
)

// mustGetOneTableFromSCC - retrieves a single table from the strongly connected component (SCC) of the subset graph.
// If the SCC contains more than one table, it panics.
func mustGetOneTableFromSCC(scc condensationgraph.SCC) commonmodels.Table {
	tables := scc.Vertexes()
	if len(tables) != 1 {
		panic(fmt.Sprintf("SCC must contain only one table got %d", len(tables)))
	}
	return tables[0]
}

// getFullTableName - returns the full table name based on the SQL dialect.
// It escapes the table name and schema name according to the dialect.
func getFullTableName(dialect Dialect, t commonmodels.Table, tableAliasMap map[int]string) string {
	alias, hasAlias := tableAliasMap[t.ID]
	if !hasAlias {
		switch dialect {
		case DialectPostgres:
			return fmt.Sprintf(`"%s"."%s"`, t.Schema, t.Name)
		case DialectMySQL:
			return fmt.Sprintf("`%s`.`%s`", t.Schema, t.Name)
		default:
			panic(fmt.Sprintf("unsupported dialect %d", dialect))
		}
	}
	// If the table has an alias, then we need to escape it according to the dialect.
	switch dialect {
	case DialectPostgres:
		return fmt.Sprintf(`"%s"`, alias)
	case DialectMySQL:
		return fmt.Sprintf("`%s`", alias)
	default:
		panic(fmt.Sprintf("unsupported dialect %d", dialect))
	}
}

// getFullColumnsName - returns the full column names based on the SQL dialect.
// It checks if the column name is an expression or not.
//
//	If it is an expression, then it should return the expression as it is.
//	If it's a column name, it escapes the column name, table name, and schema name according to the dialect.
func getFullColumnsName(dialect Dialect, t commonmodels.Table, keys []tablegraph.Key, tableAliasMap map[int]string) []string {
	res := make([]string, len(keys))
	for i, k := range keys {
		if k.Expression != "" {
			// TODO: When implemented - do not forger to replace an expression with the
			// 		 alias name if has one.
			panic("IMPLEMENT ME")
		}
		res[i] = getFullColumnName(dialect, t, k.Name, tableAliasMap)
	}
	return res
}

// getFullColumnName - returns the full column name based on the SQL dialect.
// It escapes the column name, table name, and schema name according to the dialect.
func getFullColumnName(dialect Dialect, t commonmodels.Table, c string, tableAliasMap map[int]string) string {
	alias, hasAlias := tableAliasMap[t.ID]
	if !hasAlias {
		switch dialect {
		case DialectPostgres:
			return fmt.Sprintf(`"%s"."%s"."%s"`, t.Schema, t.Name, c)
		case DialectMySQL:
			return fmt.Sprintf("`%s`.`%s`.`%s`", t.Schema, t.Name, c)
		default:
			panic(fmt.Sprintf("unsupported dialect %d", dialect))
		}
	}
	switch dialect {
	case DialectPostgres:
		return fmt.Sprintf(`"%s"."%s"`, alias, c)
	case DialectMySQL:
		return fmt.Sprintf("`%s`.`%s`", alias, c)
	default:
		panic(fmt.Sprintf("unsupported dialect %d", dialect))
	}
}

func addSubsetConditionForRoot(sb *sqlbuilder.SelectBuilder, conds []string) {
	sb.Where(
		sb.And(conds...),
	)
}

// addSubsetConditions - adds subset conditions to the SQL query.
func addSubsetConditions(
	sb *sqlbuilder.SelectBuilder,
	edge condensationgraph.Edge,
	conds []string,
	leftTableColumn []string,
) {
	if !edge.To().SCC.HasSubsetConditions() {
		// If the To table has no subset conditions, we can skip it.
		return
	}

	if !edge.OriginalEdge().IsNullable() {
		// If engage is not nullable, then we can skip the nullability check.
		sb.Where(
			sb.And(conds...),
		)
		return
	}

	/*
			If edge is nullable we have to add the nullability check.

			Let's say we are join b and a and both tables have some subset cond.
		    Table b has nullable a_id FK.

			Example query:

				SELECT *
				FROM public.b
				LEFT JOIN public.a ON b.a_id = a.id
				WHERE b.is_available = TRUE 	-- Check subset cond on b
				  AND ( 						-- Nullability check for B that is in the left join
						b.a_id IS NULL  		-- If FK is NULL we allow this record
						OR (a.title = 'test') 	-- Otherwise we have to check a.col1 subset cond
		           )
	*/

	if len(leftTableColumn) == 0 {
		panic("left table columns are empty")
	}

	// FK columns - NULL check
	var fkColumnsCheckList []string
	for _, col := range leftTableColumn {
		fkColumnsCheckList = append(fkColumnsCheckList, sb.IsNull(col))
	}

	// Combine the conditions via OR between each group and the two groups must be combined via OR.
	sb.Where(
		sb.Or(
			sb.Or(fkColumnsCheckList...),
			sb.And(conds...),
		),
	)
}

// joinCondition - represents a join condition between two tables.
type joinCondition struct {
	// Left - the left side of the join condition.
	Left string
	// Right - the right side of the join condition.
	Right string
}

func newJoinCondition(left string, right string) joinCondition {
	return joinCondition{
		Left:  left,
		Right: right,
	}
}

// joinConditions - represents a slice of joinCondition.
type joinConditions []joinCondition

// newJoinConditions - zips two slices of strings into a slice of joinCondition.
func newJoinConditions(left []string, right []string) joinConditions {
	if len(left) != len(right) {
		panic(fmt.Sprintf(
			"left and right conditions must be of the same length, got %d and %d",
			len(left), len(right)),
		)
	}

	res := make([]joinCondition, len(left))
	for i := range left {
		res[i] = newJoinCondition(left[i], right[i])
	}
	return res
}

// render - builds the SQL join conditions from the joinConditions slice.
// It returns a render representation of the join conditions united by AND operator.
func (jc joinConditions) render(sb *sqlbuilder.SelectBuilder) string {
	var conds []string
	for _, j := range jc {
		conds = append(conds, sb.Equal(j.Left, sqlbuilder.Raw(j.Right)))
	}
	return sb.And(conds...)
}

// setJoinClause - sets the join clause for the SQL query.
func setJoinClause(
	sb *sqlbuilder.SelectBuilder,
	rightTableName string,
	leftTableColumns []string,
	rightTableColumns []string,
	joinMode sqlbuilder.JoinOption,
) {
	// First render the JOIN ON conditions by using joinConditions type and render than
	joinConds := newJoinConditions(leftTableColumns, rightTableColumns).render(sb)
	sb.JoinWithOption(joinMode, rightTableName, joinConds)
}

// getTableAlias - generates a table alias based on the schema, table name, and sequence value.
// It does not escape the resulting alias, so it should be used with caution.
func getTableAlias(t commonmodels.Table, seqValue int) string {
	return fmt.Sprintf("%s_%s__%d", t.Schema, t.Name, seqValue)
}

// makeTableAliasesForDAG - generates table aliases for the tables that are ambiguous in the query.
// Use it only for condensation graph that basically is a DAG (no cycles in SCCs).
func makeTableAliasesForDAG(graph map[int][]condensationgraph.Edge) map[int]string {
	tablesEdges := make(map[int][]tablegraph.Edge)
	// Need to sort edger order to support deterministic order of the table aliases.
	sortedGraphFromKeys := make([]int, 0, len(graph))
	for k := range graph {
		sortedGraphFromKeys = append(sortedGraphFromKeys, k)
	}
	sort.Ints(sortedGraphFromKeys)
	for i := range sortedGraphFromKeys {
		edges := graph[sortedGraphFromKeys[i]]
		for _, edge := range edges {
			originalEdge := edge.OriginalEdge()
			// Save into To table the edge that is going to be used for the join.
			tablesEdges[originalEdge.To().TableID()] = append(tablesEdges[originalEdge.To().TableID()], originalEdge)
		}
	}
	res := make(map[int]string)
	for _, tableEdgesStat := range tablesEdges {
		if len(tableEdgesStat) == 1 {
			// If the table has only one edge, then we can skip it.
			continue
		}
		var aliasSequence int
		for _, edge := range tableEdgesStat {
			res[edge.ID()] = getTableAlias(edge.To().Table(), aliasSequence)
			aliasSequence++
		}
	}
	return res
}

// getSubsetConditionsWithTableAlias - get subset condition for the To table and rewrite the
// table name with the alias provided. Returns rewritten conditions.
func getSubsetConditionsWithTableAlias(
	dialect Dialect,
	table commonmodels.Table,
	alias string,
) []string {
	// If the alias is found we have to replace the table name with the alias
	// Possible cases:
	//  1. escaped only table name
	//	2. escaped only schema name
	//	3. table and schema are escaped
	var ptrn string
	switch sqlbuilder.Flavor(dialect) {
	case sqlbuilder.MySQL, sqlbuilder.ClickHouse, sqlbuilder.Doris:
		//escapeChar = "`"
		ptrn = fmt.Sprintf("`?%s`?.`?%s`?", table.Schema, table.Name)
	case sqlbuilder.PostgreSQL, sqlbuilder.SQLServer,
		sqlbuilder.SQLite, sqlbuilder.Presto,
		sqlbuilder.Oracle, sqlbuilder.Informix:
		ptrn = fmt.Sprintf(`"?%s"?."?%s"?`, table.Schema, table.Name)
		//escapeChar = `"`
	case sqlbuilder.CQL:
		ptrn = fmt.Sprintf("'?%s'?.'?%s'?", table.Schema, table.Name)
		//escapeChar = "'"
	}
	re, err := regexp.Compile(ptrn)
	if err != nil {
		panic(fmt.Sprintf("failed to compile subset condition regex: %v", err))
	}
	replacedSubsetConditions := make([]string, 0, len(table.SubsetConditions))
	for _, cond := range table.SubsetConditions {
		replacedCond := re.ReplaceAllString(cond, alias)
		replacedSubsetConditions = append(replacedSubsetConditions, replacedCond)
	}
	return replacedSubsetConditions
}
