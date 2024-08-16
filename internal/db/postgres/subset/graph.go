package subset

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/toolkit"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
)

const (
	sscVertexIsVisited    = 1
	sscVertexIsNotVisited = -1
)

var (
	foreignKeyColumnsQuery = `
		SELECT n.nspname                as             fk_table_schema,
			   fk_ref_table.relname     as             fk_table_name,
			   array_agg(curr_table_attrs.attname)     curr_table_columns,
			   bool_or(NOT attnotnull)  as 		       is_nullable
		FROM pg_catalog.pg_constraint curr_table_con
				 join pg_catalog.pg_class fk_ref_table on curr_table_con.confrelid = fk_ref_table.oid
				 join pg_catalog.pg_namespace n on fk_ref_table.relnamespace = n.oid
				 join pg_catalog.pg_attribute curr_table_attrs on curr_table_attrs.attrelid = curr_table_con.conrelid AND
																  curr_table_attrs.attnum = ANY (curr_table_con.conkey)
		WHERE curr_table_con.conrelid = $1
		  AND curr_table_con.contype = 'f'
		GROUP BY fk_table_schema, fk_table_name, curr_table_con.oid;
	`
)

// Graph - the graph representation of the DB tables. Is responsible for finding the cycles in the graph
// and searching subset Path for the tables
type Graph struct {
	// tables - the tables that are in the scope. They were previously fetched from the DB by RuntimeContext
	tables []*entries.Table
	// graph - the oriented graph representation of the DB tables
	graph [][]*Edge
	// graph - the oriented graph representation of the DB tables
	reversedGraph [][]int
	// scc - the strongly connected components in the graph
	scc []*Component
	// condensedGraph - the condensed graph representation of the DB tables
	condensedGraph [][]*CondensedEdge
	// reversedCondensedGraph - the reversed condensed graph representation of the DB tables
	reversedCondensedGraph [][]*CondensedEdge
	// componentsToOriginalVertexes - the mapping condensed graph vertexes to the original graph vertexes
	componentsToOriginalVertexes map[int][]int
	// paths - the subset paths for the tables. The key is the vertex index in the graph and the value is the path for
	// creating the subset query
	paths    map[int]*Path
	edges    []*Edge
	visited  []int
	order    []int
	sscCount int
}

// NewGraph creates a new graph based on the provided tables by finding the references in DB between them
func NewGraph(ctx context.Context, tx pgx.Tx, tables []*entries.Table) (*Graph, error) {
	graph := make([][]*Edge, len(tables))
	reversedGraph := make([][]int, len(tables))
	edges := make([]*Edge, 0)

	var edgeIdSequence int
	for idx, table := range tables {
		refs, err := getReferences(ctx, tx, table.Oid)
		if err != nil {
			return nil, fmt.Errorf("error getting references: %w", err)
		}
		for _, ref := range refs {
			foreignTableIdx := slices.IndexFunc(tables, func(t *entries.Table) bool {
				return t.Name == ref.Name && t.Schema == ref.Schema
			})

			if foreignTableIdx == -1 {
				log.Debug().
					Str("Schema", ref.Schema).
					Str("Table", ref.Name).
					Msg("unable to find foreign table: it might be excluded from the dump")
				continue
			}
			edge := NewEdge(
				edgeIdSequence,
				foreignTableIdx,
				ref.IsNullable,
				NewTableLink(idx, table, ref.ReferencedKeys),
				NewTableLink(foreignTableIdx, tables[foreignTableIdx], tables[foreignTableIdx].PrimaryKey),
			)
			graph[idx] = append(
				graph[idx],
				edge,
			)

			reversedGraph[foreignTableIdx] = append(
				reversedGraph[foreignTableIdx],
				idx,
			)
			edges = append(edges, edge)

			edgeIdSequence++
		}
	}
	return &Graph{
		tables:        tables,
		graph:         graph,
		paths:         make(map[int]*Path),
		edges:         edges,
		visited:       make([]int, len(tables)),
		order:         make([]int, 0),
		reversedGraph: reversedGraph,
	}, nil
}

// findSubsetVertexes - finds the subset vertexes in the graph
func (g *Graph) findSubsetVertexes() {
	for v := range g.condensedGraph {
		path := NewPath(v)
		var from, fullFrom []*CondensedEdge
		if len(g.scc[v].getSubsetConds()) > 0 {
			path.AddVertex(v)
		}
		g.subsetDfs(path, v, &fullFrom, &from, rootScopeId)

		if path.Len() > 0 {
			g.paths[v] = path
		}
	}
}

func (g *Graph) subsetDfs(path *Path, v int, fullFrom, from *[]*CondensedEdge, scopeId int) {
	for _, to := range g.condensedGraph[v] {
		*fullFrom = append(*fullFrom, to)
		*from = append(*from, to)
		currentScopeId := scopeId
		if len(g.scc[to.to.idx].getSubsetConds()) > 0 {
			for _, e := range *from {
				currentScopeId = path.AddEdge(e, currentScopeId)
			}
			*from = (*from)[:0]
		}
		g.subsetDfs(path, to.to.idx, fullFrom, from, currentScopeId)
		*fullFrom = (*fullFrom)[:len(*fullFrom)-1]
		if len(*from) > 0 {
			*from = (*from)[:len(*from)-1]
		}
	}
}

// findScc - finds the strongly connected components in the graph
func (g *Graph) findScc() []int {
	g.order = g.order[:0]
	g.eraseVisited()
	for v := range g.graph {
		if g.visited[v] == sscVertexIsNotVisited {
			g.topologicalSortDfs(v)
		}
	}
	slices.Reverse(g.order)

	g.eraseVisited()
	var sscCount int
	for _, v := range g.order {
		if g.visited[v] == sscVertexIsNotVisited {
			g.markComponentDfs(v, sscCount)
			sscCount++
		}
	}
	g.sscCount = sscCount
	return g.visited
}

func (g *Graph) eraseVisited() {
	for idx := range g.visited {
		g.visited[idx] = sscVertexIsNotVisited
	}
}

func (g *Graph) topologicalSortDfs(v int) {
	g.visited[v] = sscVertexIsVisited
	for _, to := range g.graph[v] {
		if g.visited[to.idx] == sscVertexIsNotVisited {
			g.topologicalSortDfs(to.idx)
		}
	}
	g.order = append(g.order, v)
}

func (g *Graph) markComponentDfs(v, component int) {
	g.visited[v] = component
	for _, to := range g.reversedGraph[v] {
		if g.visited[to] == sscVertexIsNotVisited {
			g.markComponentDfs(to, component)
		}
	}
}

func (g *Graph) buildCondensedGraph() {
	g.findScc()

	originalVertexesToComponents := g.visited
	componentsToOriginalVertexes := make(map[int][]int, g.sscCount)
	for vertexIdx, componentIdx := range originalVertexesToComponents {
		componentsToOriginalVertexes[componentIdx] = append(componentsToOriginalVertexes[componentIdx], vertexIdx)
	}
	g.componentsToOriginalVertexes = componentsToOriginalVertexes

	// 1. Collect all tables for the component
	// 2. Find all edges within the component
	condensedEdges := make(map[int]struct{})
	var ssc []*Component
	for componentIdx := 0; componentIdx < g.sscCount; componentIdx++ {

		tables := make(map[int]*entries.Table)
		for _, vertexIdx := range componentsToOriginalVertexes[componentIdx] {
			tables[vertexIdx] = g.tables[vertexIdx]
		}

		componentGraph := make(map[int][]*Edge)
		for _, vertexIdx := range componentsToOriginalVertexes[componentIdx] {
			var edges []*Edge
			for _, e := range g.graph[vertexIdx] {
				if slices.Contains(componentsToOriginalVertexes[componentIdx], e.to.idx) {
					edges = append(edges, e)
					condensedEdges[e.id] = struct{}{}
				}
			}
			componentGraph[vertexIdx] = edges
		}

		ssc = append(ssc, NewComponent(componentIdx, componentGraph, tables))
	}
	g.scc = ssc

	// 3. Build condensed graph
	g.condensedGraph = make([][]*CondensedEdge, g.sscCount)
	g.reversedCondensedGraph = make([][]*CondensedEdge, g.sscCount)
	var condensedEdgeIdxSeq int
	for _, edge := range g.edges {
		if _, ok := condensedEdges[edge.id]; ok {
			continue
		}

		fromLinkIdx := originalVertexesToComponents[edge.from.idx]
		fromLink := NewComponentLink(
			fromLinkIdx,
			ssc[fromLinkIdx],
			edge.from.keys,
			overrideKeys(edge.from.table, edge.from.keys),
		)
		toLinkIdx := originalVertexesToComponents[edge.to.idx]
		toLink := NewComponentLink(
			toLinkIdx,
			ssc[toLinkIdx],
			edge.to.keys,
			overrideKeys(edge.to.table, edge.to.keys),
		)
		condensedEdge := NewCondensedEdge(condensedEdgeIdxSeq, fromLink, toLink, edge)
		g.condensedGraph[fromLinkIdx] = append(g.condensedGraph[fromLinkIdx], condensedEdge)
		reversedEdges := NewCondensedEdge(condensedEdgeIdxSeq, toLink, fromLink, edge)
		g.reversedCondensedGraph[toLinkIdx] = append(g.reversedCondensedGraph[toLinkIdx], reversedEdges)
		condensedEdgeIdxSeq++
	}
}

func (g *Graph) generateAndSetQueryForTable(path *Path) {
	// We start DFS from the root scope
	rootVertex := g.scc[path.rootVertex]
	table := rootVertex.getOneTable()
	query := g.generateQueriesDfs(path, nil)
	table.Query = query
}

func (g *Graph) generateAndSetQueryForScc(path *Path) {
	// We start DFS from the root scope
	rootVertex := g.scc[path.rootVertex]
	cq := newCteQuery(rootVertex)
	g.generateQueriesSccDfs(cq, path, nil)
	for _, t := range rootVertex.tables {
		query := cq.generateQuery(t)
		t.Query = query
	}
}

func (g *Graph) generateQueriesSccDfs(cq *cteQuery, path *Path, scopeEdge *ScopeEdge) {
	scopeId := rootScopeId
	if scopeEdge != nil {
		scopeId = scopeEdge.scopeId
	}
	if len(path.scopeEdges[scopeId]) == 0 && scopeEdge != nil {
		return
	}

	g.generateQueryForScc(cq, scopeId, path, scopeEdge)
	for _, nextScopeEdge := range path.scopeGraph[scopeId] {
		g.generateQueriesSccDfs(cq, path, nextScopeEdge)
	}
}

func (g *Graph) generateQueryForScc(cq *cteQuery, scopeId int, path *Path, prevScopeEdge *ScopeEdge) {
	edges := path.scopeEdges[scopeId]
	nextScopeEdges := path.scopeGraph[scopeId]
	rootVertex := g.scc[path.rootVertex]
	if prevScopeEdge != nil {
		// If prevScopeEdge != nil then we have subquery
		edges = edges[1:]
		rootVertex = prevScopeEdge.originalCondensedEdge.to.component
	}
	if len(rootVertex.cycles) > 1 {
		panic("IMPLEMENT ME: more than one cycle found in SCC")
	}

	cycle := orderCycle(rootVertex.cycles[0], edges, path.scopeGraph[scopeId])
	g.generateRecursiveQueriesForCycle(cq, scopeId, cycle, edges, nextScopeEdges)
	g.generateQueriesForVertexesInCycle(cq, scopeId, cycle)
}

func (g *Graph) generateQueriesForVertexesInCycle(cq *cteQuery, scopeId int, cycle []*Edge) {
	for _, t := range getTablesFromCycle(cycle) {
		queryName := fmt.Sprintf("%s__%s__ids", t.Schema, t.Name)
		query := generateAllTablesValidPkSelection(cycle, scopeId, t)
		cq.addItem(queryName, query)
	}
}

func (g *Graph) generateRecursiveQueriesForCycle(
	cq *cteQuery, scopeId int, cycle []*Edge, rest []*CondensedEdge, nextScopeEdges []*ScopeEdge,
) {
	var (
		cycleId              = getCycleIdent(cycle)
		overriddenTableNames = make(map[toolkit.Oid]string)
	)

	rest = slices.Clone(rest)
	for _, se := range nextScopeEdges {
		t := se.originalCondensedEdge.originalEdge.to.table
		overriddenTableNames[t.Oid] = fmt.Sprintf("%s__%s__ids", t.Schema, t.Name)
		rest = append(rest, se.originalCondensedEdge)
	}

	//var unionQueries []string
	shiftedCycle := slices.Clone(cycle)
	for idx := 1; idx <= len(cycle); idx++ {
		var (
			mainTable = shiftedCycle[0].from.table
			// queryName - name of a query in the recursive CTE
			// where:
			//   * s - scope id
			//   * c - cycle id
			//   * pt1 - part 1 of the recursive query
			queryName         = fmt.Sprintf("__s%d__c%s__%s__%s", scopeId, cycleId, mainTable.Schema, mainTable.Name)
			filteredQueryName = fmt.Sprintf("%s__filtered", queryName)
		)

		query := generateQuery(queryName, shiftedCycle, rest, overriddenTableNames)
		cq.addItem(queryName, query)
		filteredQuery := generateIntegrityCheckJoinConds(shiftedCycle, mainTable, queryName)
		cq.addItem(filteredQueryName, filteredQuery)
		shiftedCycle = shiftCycle(shiftedCycle)
	}
}

func (g *Graph) generateQueriesDfs(path *Path, scopeEdge *ScopeEdge) string {
	// TODO:
	// 		1. Add scopeEdges support and LEFT JOIN
	//		2. Consider how to implement LEFT JOIN for WHERE IN clause (maybe use cond ISNULL OR IN)
	scopeId := rootScopeId
	if scopeEdge != nil {
		scopeId = scopeEdge.scopeId
	}
	if len(path.scopeEdges[scopeId]) == 0 && scopeEdge != nil {
		return ""
	}

	currentScopeQuery := g.generateQueryForTables(path, scopeEdge)
	var subQueries []string
	for _, nextScope := range path.scopeGraph[scopeId] {
		subQuery := g.generateQueriesDfs(path, nextScope)
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

func (g *Graph) generateQueryForTables(path *Path, scopeEdge *ScopeEdge) string {
	scopeId := rootScopeId
	if scopeEdge != nil {
		scopeId = scopeEdge.scopeId
	}
	var edges []*Edge
	for _, se := range path.scopeEdges[scopeId] {
		edges = append(edges, se.originalEdge)
	}

	// Use root table as a root table from path
	rootVertex := g.scc[path.rootVertex]
	rootTable := rootVertex.getOneTable()
	if scopeEdge != nil {
		// If it is not a root scope use the right table from the first edge as a root table
		// And left table from the first edge as a left table for the subquery. It will be used for where in clause
		rootTable = scopeEdge.originalCondensedEdge.originalEdge.to.table
		edges = edges[1:]
	}

	whereConds := slices.Clone(rootTable.SubsetConds)
	selectClause := fmt.Sprintf(`SELECT "%s"."%s".*`, rootTable.Schema, rootTable.Name)
	if scopeEdge != nil {
		selectClause = generateSelectByPrimaryKey(rootTable, rootTable.PrimaryKey)
	}
	fromClause := fmt.Sprintf(`FROM "%s"."%s" `, rootTable.Schema, rootTable.Name)

	var joinClauses []string

	nullabilityMap := make(map[int]bool)
	for _, e := range edges {
		isNullable := e.isNullable
		if !isNullable {
			isNullable = nullabilityMap[e.from.idx]
		}
		nullabilityMap[e.to.idx] = isNullable
		joinType := joinTypeInner
		if isNullable {
			joinType = joinTypeLeft
		}
		joinClause := generateJoinClauseV2(e, joinType, make(map[toolkit.Oid]string))
		joinClauses = append(joinClauses, joinClause)
	}
	integrityChecks := generateIntegrityChecksForNullableEdges(nullabilityMap, edges, make(map[toolkit.Oid]string))
	whereConds = append(whereConds, integrityChecks...)

	query := fmt.Sprintf(
		`%s %s %s %s`,
		selectClause,
		fromClause,
		strings.Join(joinClauses, " "),
		generateWhereClause(whereConds),
	)

	if scopeEdge != nil {
		var leftTableConds []string
		originalEdge := scopeEdge.originalCondensedEdge.originalEdge
		for _, k := range originalEdge.from.keys {
			leftTable := originalEdge.from.table
			leftTableConds = append(leftTableConds, fmt.Sprintf(`"%s"."%s"."%s"`, leftTable.Schema, leftTable.Name, k))
		}
		query = fmt.Sprintf("((%s) IN (%s))", strings.Join(leftTableConds, ", "), query)

		if scopeEdge.isNullable {
			var nullableChecks []string
			for _, k := range originalEdge.from.keys {
				nullableCheck := fmt.Sprintf(`"%s"."%s"."%s" IS NULL`, originalEdge.from.table.Schema, originalEdge.from.table.Name, k)
				nullableChecks = append(nullableChecks, nullableCheck)
			}
			query = fmt.Sprintf(
				"((%s) OR %s)",
				strings.Join(nullableChecks, " AND "),
				query,
			)
		}

	}

	return query
}

func (g *Graph) GetSortedTablesAndDependenciesGraph() ([]toolkit.Oid, map[toolkit.Oid][]toolkit.Oid) {
	condensedEdges := sortCondensedEdges(g.reversedCondensedGraph)
	var tables []toolkit.Oid
	dependenciesGraph := make(map[toolkit.Oid][]toolkit.Oid)
	for _, condEdgeIdx := range condensedEdges {
		edge := g.scc[condEdgeIdx]
		var componentTables []toolkit.Oid
		for _, t := range edge.tables {
			componentTables = append(componentTables, t.Oid)
		}
		tables = append(tables, componentTables...)
	}

	for idx, edge := range g.reversedCondensedGraph {
		for _, srcTable := range g.scc[idx].tables {
			dependenciesGraph[srcTable.Oid] = make([]toolkit.Oid, 0)
		}

		for _, e := range edge {
			for _, srcTable := range e.to.component.tables {
				for _, dstTable := range e.from.component.tables {
					dependenciesGraph[srcTable.Oid] = append(dependenciesGraph[srcTable.Oid], dstTable.Oid)
				}
			}
		}
	}

	slices.Reverse(tables)

	return tables, dependenciesGraph
}

func getReferences(ctx context.Context, tx pgx.Tx, tableOid toolkit.Oid) ([]*toolkit.Reference, error) {
	var refs []*toolkit.Reference
	rows, err := tx.Query(ctx, foreignKeyColumnsQuery, tableOid)
	if err != nil {
		return nil, fmt.Errorf("error executing ForeignKeyColumnsQuery: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		ref := &toolkit.Reference{}
		if err = rows.Scan(&ref.Schema, &ref.Name, &ref.ReferencedKeys, &ref.IsNullable); err != nil {
			return nil, fmt.Errorf("error scanning ForeignKeyColumnsQuery: %w", err)
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

func overrideKeys(table *entries.Table, keys []string) []string {
	var res []string
	for _, k := range keys {
		res = append(res, fmt.Sprintf(`"%s.%s.%s"`, table.Schema, table.Name, k))
	}
	return res
}

func isPathForScc(path *Path, graph *Graph) bool {
	return graph.scc[path.rootVertex].hasCycle()
}

func orderCycle(cycle []*Edge, subsetJoins []*CondensedEdge, scopeEdges []*ScopeEdge) []*Edge {
	var (
		vertexes         []int
		valuableEdgesIdx int
	)

	for _, e := range cycle {
		vertexes = append(vertexes, e.from.idx)
	}

	for _, sj := range subsetJoins {
		if slices.Contains(vertexes, sj.originalEdge.from.idx) {
			valuableEdgesIdx = slices.IndexFunc(cycle, func(e *Edge) bool {
				return sj.originalEdge.from.idx == e.from.idx
			})
			if !sj.originalEdge.isNullable {
				break
			}
		}
	}

	for _, se := range scopeEdges {
		if slices.Contains(vertexes, se.originalCondensedEdge.from.idx) {
			valuableEdgesIdx = slices.IndexFunc(cycle, func(e *Edge) bool {
				return se.originalCondensedEdge.originalEdge.from.idx == e.from.idx
			})
			if !se.originalCondensedEdge.originalEdge.isNullable {
				break
			}
		}
	}

	if valuableEdgesIdx == -1 {
		panic("is not found")
	}

	resCycle := slices.Clone(cycle[valuableEdgesIdx:])
	resCycle = append(resCycle, cycle[:valuableEdgesIdx]...)
	return resCycle
}

func generateQuery(queryName string, cycle []*Edge, rest []*CondensedEdge, overriddenTables map[toolkit.Oid]string) string {
	var (
		selectKeys                             []string
		initialJoins, recursiveJoins           []string
		initialWhereConds, recursiveWhereConds []string
		integrityCheck                         string
		cycleSubsetConds                       []string
		edges                                  = slices.Clone(cycle[:len(cycle)-1])
		droppedEdge                            = cycle[len(cycle)-1]
	)
	for _, ce := range rest {
		edges = append(edges, ce.originalEdge)
	}

	for _, t := range getTablesFromCycle(cycle) {
		var keysWithAliases []string
		for _, k := range t.PrimaryKey {
			keysWithAliases = append(keysWithAliases, fmt.Sprintf(`"%s"."%s"."%s" as "%s__%s__%s"`, t.Schema, t.Name, k, t.Schema, t.Name, k))
		}
		selectKeys = append(selectKeys, keysWithAliases...)
		if len(t.SubsetConds) > 0 {
			cycleSubsetConds = append(cycleSubsetConds, t.SubsetConds...)
		}
	}

	var droppedKeysWithAliases []string
	for _, k := range droppedEdge.from.keys {
		t := droppedEdge.from.table
		droppedKeysWithAliases = append(droppedKeysWithAliases, fmt.Sprintf(`"%s"."%s"."%s" as "%s__%s__%s"`, t.Schema, t.Name, k, t.Schema, t.Name, k))
	}
	selectKeys = append(selectKeys, droppedKeysWithAliases...)

	var initialPathSelectionKeys []string
	for _, k := range edges[0].from.table.PrimaryKey {
		t := edges[0].from.table
		pathName := fmt.Sprintf(
			`ARRAY["%s"."%s"."%s"] AS %s__%s__%s__path`,
			t.Schema, t.Name, k,
			t.Schema, t.Name, k,
		)
		initialPathSelectionKeys = append(initialPathSelectionKeys, pathName)
	}

	initialKeys := slices.Clone(selectKeys)
	initialKeys = append(initialKeys, initialPathSelectionKeys...)
	initFromClause := fmt.Sprintf(`FROM "%s"."%s" `, edges[0].from.table.Schema, edges[0].from.table.Name)
	integrityCheck = "TRUE AS valid"
	initialKeys = append(initialKeys, integrityCheck)
	initialWhereConds = append(initialWhereConds, cycleSubsetConds...)

	initialSelect := fmt.Sprintf("SELECT %s", strings.Join(initialKeys, ", "))
	nullabilityMap := make(map[int]bool)
	for _, e := range edges {
		isNullable := e.isNullable
		if !isNullable {
			isNullable = nullabilityMap[e.from.idx]
		}
		nullabilityMap[e.to.idx] = isNullable
		joinType := joinTypeInner
		if isNullable {
			joinType = joinTypeLeft
		}
		initialJoins = append(initialJoins, generateJoinClauseV2(e, joinType, overriddenTables))
	}

	integrityChecks := generateIntegrityChecksForNullableEdges(nullabilityMap, edges, overriddenTables)
	initialWhereConds = append(initialWhereConds, integrityChecks...)
	initialWhereClause := generateWhereClause(initialWhereConds)
	initialQuery := fmt.Sprintf(`%s %s %s %s`,
		initialSelect, initFromClause, strings.Join(initialJoins, " "), initialWhereClause,
	)

	recursiveIntegrityChecks := slices.Clone(cycleSubsetConds)
	recursiveIntegrityChecks = append(recursiveIntegrityChecks, integrityChecks...)
	recursiveIntegrityCheck := fmt.Sprintf("(%s) AS valid", strings.Join(recursiveIntegrityChecks, " AND "))
	recursiveKeys := slices.Clone(selectKeys)
	for _, k := range edges[0].from.table.PrimaryKey {
		t := edges[0].from.table
		//recursivePathSelectionKeys = append(recursivePathSelectionKeys, fmt.Sprintf(`coalesce("%s"."%s"."%s"::TEXT, 'NULL')`, t.Schema, t.Name, k))

		pathName := fmt.Sprintf(
			`"%s__%s__%s__path" || ARRAY["%s"."%s"."%s"]`,
			t.Schema, t.Name, k,
			t.Schema, t.Name, k,
		)
		recursiveKeys = append(recursiveKeys, pathName)
	}
	recursiveKeys = append(recursiveKeys, recursiveIntegrityCheck)

	recursiveSelect := fmt.Sprintf("SELECT %s", strings.Join(recursiveKeys, ", "))
	recursiveFromClause := fmt.Sprintf(`FROM "%s" `, queryName)
	recursiveJoins = append(recursiveJoins, generateJoinClauseForDroppedEdge(droppedEdge, queryName))
	nullabilityMap = make(map[int]bool)
	for _, e := range edges {
		isNullable := e.isNullable
		if !isNullable {
			isNullable = nullabilityMap[e.from.idx]
		}
		nullabilityMap[e.to.idx] = isNullable
		joinType := joinTypeInner
		if isNullable {
			joinType = joinTypeLeft
		}
		recursiveJoins = append(recursiveJoins, generateJoinClauseV2(e, joinType, overriddenTables))
	}

	recursiveValidCond := fmt.Sprintf(`"%s"."%s"`, queryName, "valid")
	recursiveWhereConds = append(recursiveWhereConds, recursiveValidCond)
	for _, k := range edges[0].from.table.PrimaryKey {
		t := edges[0].from.table

		recursivePathCheck := fmt.Sprintf(
			`NOT "%s"."%s"."%s" = ANY("%s"."%s__%s__%s__%s")`,
			t.Schema, t.Name, k,
			queryName, t.Schema, t.Name, k, "path",
		)

		recursiveWhereConds = append(recursiveWhereConds, recursivePathCheck)
	}
	recursiveWhereClause := generateWhereClause(recursiveWhereConds)

	recursiveQuery := fmt.Sprintf(`%s %s %s %s`,
		recursiveSelect, recursiveFromClause, strings.Join(recursiveJoins, " "), recursiveWhereClause,
	)

	query := fmt.Sprintf("( %s ) UNION ( %s )", initialQuery, recursiveQuery)
	return query
}

func getTablesFromCycle(cycle []*Edge) (res []*entries.Table) {
	for _, e := range cycle {
		res = append(res, e.to.table)
	}
	slices.SortFunc(res, func(a, b *entries.Table) int {
		return cmp.Compare(a.Oid, b.Oid)
	})
	return res
}

func generateIntegrityChecksForNullableEdges(nullabilityMap map[int]bool, edges []*Edge, overriddenTables map[toolkit.Oid]string) (res []string) {
	// generate conditional checks for foreign tables that has left joins

	for _, e := range edges {
		if isNullable := nullabilityMap[e.to.idx]; !isNullable {
			continue
		}
		var keys []string
		for idx := range e.from.keys {
			leftTableKey := e.from.keys
			rightTableKey := e.to.keys
			k := fmt.Sprintf(
				`("%s"."%s"."%s" IS NULL OR "%s"."%s"."%s" IS NOT NULL)`,
				e.from.table.Schema,
				e.from.table.Name,
				leftTableKey[idx],
				e.to.table.Schema,
				e.to.table.Name,
				rightTableKey[idx],
			)
			if _, ok := overriddenTables[e.to.table.Oid]; ok {
				k = fmt.Sprintf(
					`("%s"."%s"."%s" IS NULL OR "%s"."%s" IS NOT NULL)`,
					e.from.table.Schema,
					e.from.table.Name,
					leftTableKey[idx],
					overriddenTables[e.to.table.Oid],
					rightTableKey[idx],
				)
			}
			keys = append(keys, k)
		}
		res = append(res, fmt.Sprintf("(%s)", strings.Join(keys, " AND ")))
	}
	return
}

func generateIntegrityCheckJoinConds(cycle []*Edge, table *entries.Table, tableName string) string {

	var (
		allPks           []string
		mainTablePks     []string
		unnestSelections []string
	)

	for _, t := range getTablesFromCycle(cycle) {
		for _, k := range t.PrimaryKey {
			key := fmt.Sprintf(`"%s"."%s__%s__%s"`, tableName, t.Schema, t.Name, k)
			allPks = append(allPks, key)
			if t.Oid == table.Oid {
				pathName := fmt.Sprintf(`"%s"."%s__%s__%s__path"`, tableName, t.Schema, t.Name, k)
				mainTablePks = append(mainTablePks, key)
				unnestSelection := fmt.Sprintf(`unnest(%s) AS "%s"`, pathName, k)
				unnestSelections = append(unnestSelections, unnestSelection)
			}
		}
	}

	unnestQuery := fmt.Sprintf(
		`SELECT %s FROM "%s" WHERE NOT "%s"."valid"`,
		strings.Join(unnestSelections, ", "),
		tableName,
		tableName,
	)

	filteredQuery := fmt.Sprintf(
		`SELECT DISTINCT %s FROM "%s" WHERE (%s) NOT IN (%s)`,
		strings.Join(allPks, ", "),
		tableName,
		strings.Join(mainTablePks, ", "),
		unnestQuery,
	)

	return filteredQuery
}

func generateAllTablesValidPkSelection(cycle []*Edge, scopeId int, forTable *entries.Table) string {

	var unionParts []string

	for _, t := range getTablesFromCycle(cycle) {
		var (
			selectionKeys     []string
			cycleId           = getCycleIdent(cycle)
			filteredQueryName = fmt.Sprintf("__s%d__c%s__%s__%s__filtered", scopeId, cycleId, t.Schema, t.Name)
		)

		for _, k := range forTable.PrimaryKey {
			key := fmt.Sprintf(`"%s"."%s__%s__%s" AS "%s"`, filteredQueryName, forTable.Schema, forTable.Name, k, k)
			selectionKeys = append(selectionKeys, key)
		}

		query := fmt.Sprintf(
			`SELECT DISTINCT %s FROM "%s"`,
			strings.Join(selectionKeys, ", "),
			filteredQueryName,
		)
		unionParts = append(unionParts, query)
	}
	res := strings.Join(unionParts, " UNION ")
	return res
}

func shiftCycle(cycle []*Edge) (res []*Edge) {
	res = append(res, cycle[len(cycle)-1])
	res = append(res, cycle[:len(cycle)-1]...)
	return
}
