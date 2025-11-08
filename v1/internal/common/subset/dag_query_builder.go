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
	"github.com/huandu/go-sqlbuilder"

	"github.com/greenmaskio/greenmask/v1/internal/common/subset/condensationgraph"
)

const (
	joinPartRight = "right"
	joinPartLeft  = "left"
)

// dagQueryBuilder - builds a query for subsetGraph that does not contain cycles (directed acyclic graph).
//
// Basically, the subset graph is a condensation graph where each strongly connected component (SCC)
// contains only one vertex and does not contain cycles.
//
// The algorithm is as follows:
//
//   - Apply simple DFS algorithm from sg.rootVertex
//
//   - Keep the query scope in the stack. The scope is the subquery number. Each scope must contain
//     the table only once. If the table is ambiguous, then the query with the table that is mentioned again
//     must be decomposed into the subquery with WHERE IN join analogue.
//
//   - The Nullable FK must be involved into integrity check in two steps:
//
//     1. Check if the FK table (the left table) is NULL - if is null it's allowed into selection unless
//     id does not have the subset condition for this table.
//     2. If FK table (the left table) is NOT NULL then the value of joined PK column value (of the right table)
//     can't be NULL - meaning it must be in the selection.
//
//     It must result in the query that is similar to the following:
//
//     "sales"."customer"."storeid" IS NULL OR "sales"."store"."businessentityid" IS NOT NULL
type dagQueryBuilder struct {
	sg      *subsetGraph
	dialect Dialect
	// aliasMap - contains the aliases for the tables that are ambiguous in the query.
	// The key is a tablegraph.Edge.ID() (edge ID) and the value is an alias to use in the query.
	aliasMap map[int]string
}

func newDAGQueryBuilder(sg *subsetGraph, dialect Dialect) dagQueryBuilder {
	aliasMap := makeTableAliasesForDAG(sg.graph)
	return dagQueryBuilder{
		sg:       sg,
		dialect:  dialect,
		aliasMap: aliasMap,
	}
}

// build - builds the query for the subset graph and returns the query for all tables in one SCC.
func (b dagQueryBuilder) build() (map[int]string, error) {
	res := make(map[int]string)
	rootTable := mustGetOneTableFromSCC(b.sg.vertexes[b.sg.rootVertex])

	tableAliasMap := make(map[int]string)
	// Build the main select ... from clause for root table
	sb := sqlbuilder.PostgreSQL.
		NewSelectBuilder().
		Select("*").
		From(getFullTableName(b.dialect, rootTable, tableAliasMap))

	// Add subset conditions to the query
	if rootTable.HasSubsetConditions() {
		addSubsetConditionForRoot(sb, rootTable.SubsetConditions)
	}

	// Join all other tables in subset graph
	b.buildQueryDFS(b.sg.rootVertex, sb, tableAliasMap)

	res[rootTable.ID] = sb.String()
	return res, nil
}

// buildQueryDFS - builds the query using depth-first search (DFS) algorithm.
func (b dagQueryBuilder) buildQueryDFS(
	v int,
	sb *sqlbuilder.SelectBuilder,
	tableAliasMap map[int]string,
) {
	for _, edge := range b.sg.graph[v] {
		_, hasTableAlias := b.aliasMap[edge.OriginalEdge().ID()]
		if hasTableAlias {
			// If edge has an alias then add it to the tableAliasMap by the tableID
			// Set an alias for table ID by the edge ID
			tableAliasMap[edge.OriginalEdge().To().TableID()] = b.aliasMap[edge.OriginalEdge().ID()]
		}
		joinEdge(b.dialect, sb, edge, tableAliasMap)
		// Recursively render the query for the next vertex
		b.buildQueryDFS(edge.To().SCC.ID(), sb, tableAliasMap)
		if hasTableAlias {
			// If the edge has an alias then remove it from the tableAliasMap by the To tableID
			delete(tableAliasMap, edge.OriginalEdge().To().TableID())
		}
	}
}

func getFullColumnNames(
	dialect Dialect,
	joinPart string,
	edge condensationgraph.Edge,
	tableAliasMap map[int]string,
) []string {
	scc := edge.From().SCC
	keys := edge.OriginalEdge().From().Keys()
	if joinPart == joinPartRight {
		scc = edge.To().SCC
		keys = edge.OriginalEdge().To().Keys()
	}
	table := mustGetOneTableFromSCC(scc)
	fullColumnNames := getFullColumnsName(
		dialect,
		table,
		keys,
		tableAliasMap,
	)
	return fullColumnNames
}

// joinEdge - joins the edge into the query.
//
// tableAliasMap contains the tables aliases that were joined before.
func joinEdge(
	dialect Dialect,
	sb *sqlbuilder.SelectBuilder,
	edge condensationgraph.Edge,
	tableAliasMap map[int]string,
) {
	originalEdge := edge.OriginalEdge()
	// Determine JOIN mode (LEFT of INNER)
	joinMode := sqlbuilder.InnerJoin
	if originalEdge.IsNullable() {
		joinMode = sqlbuilder.LeftJoin
	}

	// Get Left table all columns involved into join condition.
	// The full name is not required because it uses in the previous join clause.
	leftTableFullColumnNames := getFullColumnNames(dialect, joinPartLeft, edge, tableAliasMap)

	// Get Right table full name, and all columns involved into join condition.
	rightTable := mustGetOneTableFromSCC(edge.To().SCC)
	rightTableFullName := getFullTableName(dialect, rightTable, tableAliasMap)
	rightTableFullColumnNames := getFullColumnNames(dialect, joinPartRight, edge, tableAliasMap)

	// Set the join
	setJoinClause(sb, rightTableFullName, leftTableFullColumnNames, rightTableFullColumnNames, joinMode)
	// Set subset conditions if any.
	// If subset involved into left join we have to add nullability check.
	setSubsetConditions(
		dialect,
		sb,
		edge,
		leftTableFullColumnNames,
		tableAliasMap,
	)
}

// setSubsetConditions - set subset condition for the To table.
//
// If the To table has subset conditions, then the subset conditions
// are added to the where clause.
func setSubsetConditions(
	dialect Dialect,
	sb *sqlbuilder.SelectBuilder,
	edge condensationgraph.Edge,
	leftTableColumn []string,
	tableAliasMap map[int]string,
) {
	table := edge.OriginalEdge().To().Table()
	if !table.HasSubsetConditions() {
		return
	}
	revisedSubsetConds := table.SubsetConditions
	alias, hasAlias := tableAliasMap[table.ID]
	if hasAlias {
		revisedSubsetConds = getSubsetConditionsWithTableAlias(dialect, table, alias)
		return
	}
	addSubsetConditions(sb, edge, revisedSubsetConds, leftTableColumn)
}
