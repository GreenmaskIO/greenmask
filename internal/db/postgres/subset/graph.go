package subset

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/domains"
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
			   array_agg(curr_table_attrs.attname ORDER BY array_position(curr_table_con.conkey, curr_table_attrs.attnum))     curr_table_columns,
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
	// reversedGraph - the reversed oriented graph representation of the DB tables
	reversedGraph [][]*Edge
	// graph - the oriented graph representation of the DB tables
	reversedSimpleGraph [][]int
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
func NewGraph(
	ctx context.Context, tx pgx.Tx, tables []*entries.Table, vr []*domains.VirtualReference,
) (*Graph, error) {
	graph := make([][]*Edge, len(tables))
	reversedGraph := make([][]*Edge, len(tables))
	reversedSimpleGraph := make([][]int, len(tables))
	edges := make([]*Edge, 0)

	var edgeIdSequence int
	for idx, table := range tables {
		refs, err := getReferences(ctx, tx, table.Oid)
		if err != nil {
			return nil, fmt.Errorf("error getting references: %w", err)
		}
		for _, ref := range refs {
			referenceTableIdx := slices.IndexFunc(tables, func(t *entries.Table) bool {
				return t.Name == ref.Name && t.Schema == ref.Schema
			})

			if referenceTableIdx == -1 {
				log.Debug().
					Str("Schema", ref.Schema).
					Str("Table", ref.Name).
					Msg("unable to find reference table (primary): it might be excluded from the dump")
				continue
			}
			edge := NewEdge(
				edgeIdSequence,
				referenceTableIdx,
				ref.IsNullable,
				NewTableLink(idx, table, NewKeysByColumn(ref.ReferencedKeys), nil),
				NewTableLink(referenceTableIdx, tables[referenceTableIdx], NewKeysByColumn(tables[referenceTableIdx].PrimaryKey), nil),
			)
			graph[idx] = append(
				graph[idx],
				edge,
			)

			reversedEdge := NewEdge(
				edgeIdSequence,
				idx,
				ref.IsNullable,
				NewTableLink(referenceTableIdx, tables[referenceTableIdx], NewKeysByColumn(tables[referenceTableIdx].PrimaryKey), nil),
				NewTableLink(idx, table, NewKeysByColumn(ref.ReferencedKeys), nil),
			)

			reversedGraph[referenceTableIdx] = append(
				reversedGraph[referenceTableIdx],
				reversedEdge,
			)

			reversedSimpleGraph[referenceTableIdx] = append(
				reversedSimpleGraph[referenceTableIdx],
				idx,
			)
			edges = append(edges, edge)

			edgeIdSequence++
		}

		for _, ref := range getVirtualReferences(vr, table) {

			referenceTableIdx := slices.IndexFunc(tables, func(t *entries.Table) bool {
				return t.Name == ref.Name && t.Schema == ref.Schema
			})

			if referenceTableIdx == -1 {
				log.Debug().
					Str("Schema", ref.Schema).
					Str("Table", ref.Name).
					Msg("unable to find reference table (primary): it might be excluded from the dump")
				continue
			}

			edge := NewEdge(
				edgeIdSequence,
				referenceTableIdx,
				!ref.NotNull,
				NewTableLink(idx, table, NewKeysByReferencedColumn(ref.Columns), ref.PolymorphicExprs),
				NewTableLink(referenceTableIdx, tables[referenceTableIdx], NewKeysByColumn(tables[referenceTableIdx].PrimaryKey), nil),
			)
			graph[idx] = append(
				graph[idx],
				edge,
			)

			reversedSimpleGraph[referenceTableIdx] = append(
				reversedSimpleGraph[referenceTableIdx],
				idx,
			)
			edges = append(edges, edge)

			edgeIdSequence++

		}
	}
	g := &Graph{
		tables:              tables,
		graph:               graph,
		paths:               make(map[int]*Path),
		edges:               edges,
		visited:             make([]int, len(tables)),
		order:               make([]int, 0),
		reversedSimpleGraph: reversedSimpleGraph,
		reversedGraph:       reversedGraph,
	}
	g.buildCondensedGraph()
	return g, nil
}

func (g *Graph) Tables() []*entries.Table {
	return g.tables
}

func (g *Graph) ReversedGraph() [][]*Edge {
	return g.reversedGraph
}

func (g *Graph) GetTables() []*entries.Table {
	return g.tables
}

func (g *Graph) GetCycles() [][]*Edge {
	var cycles [][]*Edge
	for _, c := range g.scc {
		if c.hasCycle() {
			cycles = append(cycles, c.cycles...)
		}
	}
	return cycles
}

func (g *Graph) GetCycledTables() (res [][]string) {
	cycles := g.GetCycles()
	for _, c := range cycles {
		var tables []string
		for _, e := range c {
			tables = append(tables, fmt.Sprintf(`%s.%s`, e.from.table.Schema, e.from.table.Name))
		}
		tables = append(tables, fmt.Sprintf(`%s.%s`, c[len(c)-1].to.table.Schema, c[len(c)-1].to.table.Name))
		res = append(res, tables)
	}
	return res
}

// findSubsetVertexes - finds the subset vertexes in the graph
func (g *Graph) findSubsetVertexes() {
	for v := range g.condensedGraph {
		path := NewPath(v)
		var from, fullFrom []*CondensedEdge
		if len(g.scc[v].getSubsetConds()) > 0 || g.scc[v].hasPolymorphicExpressions() {
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
		if len(g.scc[to.to.idx].getSubsetConds()) > 0 || (*fullFrom)[len(*fullFrom)-1].hasPolymorphicExpressions() {
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
	for _, to := range g.reversedSimpleGraph[v] {
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
		)
		toLinkIdx := originalVertexesToComponents[edge.to.idx]
		toLink := NewComponentLink(
			toLinkIdx,
			ssc[toLinkIdx],
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
	//cycle := orderCycle(rootVertex.cycles[0], edges, path.scopeGraph[scopeId])
	if len(rootVertex.groupedCycles) > 1 {
		panic("IMPLEMENT ME: more than one cycle group found in SCC")
	}
	cycleGroup := rootVertex.getOneCycleGroup()
	overlapMap := g.getOverlapMap(cycleGroup)
	for _, cycle := range cycleGroup {
		g.generateRecursiveQueriesForCycle(cq, scopeId, cycle, edges, nextScopeEdges, overlapMap)
	}

	g.generateFilteredQueries(cq, cycleGroup, scopeId)
	g.generateQueriesForVertexesInCycle(cq, scopeId, cycleGroup)
}

func (g *Graph) getOverlapMap(cycles [][]*Edge) map[string][][]*Edge {
	cyclesOverlap := make(map[string][][]*Edge, len(cycles))
	for i, currCycle := range cycles {
		cycleId := getCycleId(currCycle)
		var overlapCycles [][]*Edge
		for j, overlapCycle := range cycles {
			if i == j {
				continue
			}
			overlapCycles = append(overlapCycles, overlapCycle)
		}
		cyclesOverlap[cycleId] = overlapCycles
	}
	return cyclesOverlap
}

func (g *Graph) generateQueriesForVertexesInCycle(cq *cteQuery, scopeId int, cycles [][]*Edge) {
	for _, t := range getTablesFromCycle(cycles[0]) {
		queryName := fmt.Sprintf("%s__%s__ids", t.Schema, t.Name)
		query := generateAllTablesValidPkSelection(cycles, scopeId, t)
		cq.addItem(queryName, query)
	}
}

func (g *Graph) generateRecursiveQueriesForCycle(
	cq *cteQuery, scopeId int, cycle []*Edge, rest []*CondensedEdge, nextScopeEdges []*ScopeEdge,
	overlapMap map[string][][]*Edge,
) {
	overriddenTableNames := make(map[toolkit.Oid]string)
	rest = slices.Clone(rest)

	for _, se := range nextScopeEdges {
		t := se.originalCondensedEdge.originalEdge.to.table
		overriddenTableNames[t.Oid] = fmt.Sprintf("%s__%s__ids", t.Schema, t.Name)
		rest = append(rest, se.originalCondensedEdge)
	}

	//var unionQueries []string
	shiftedCycle := slices.Clone(cycle)
	for idx := 1; idx <= len(cycle); idx++ {
		queryName := getCycleQueryName(scopeId, shiftedCycle, "")
		query := generateQuery(queryName, shiftedCycle, rest, overriddenTableNames)
		cq.addItem(queryName, query)
		cycleId := getCycleId(shiftedCycle)
		if len(overlapMap[cycleId]) > 0 {
			overlapQueryName := getCycleQueryName(scopeId, shiftedCycle, "overlap")
			overlapQuery := generateOverlapQuery(scopeId, overlapQueryName, shiftedCycle, rest, overriddenTableNames, overlapMap[cycleId])
			cq.addItem(overlapQueryName, overlapQuery)
		}
		shiftedCycle = shiftCycle(shiftedCycle)
	}
}

func (g *Graph) generateFilteredQueries(cq *cteQuery, groupedCycles [][]*Edge, scopeId int) {

	// Clone cycles group
	cycles := make([][]*Edge, 0, len(groupedCycles))
	for _, cycle := range groupedCycles {
		cycles = append(cycles, slices.Clone(cycle))
	}
	for idx := 1; idx <= len(cycles[0]); idx++ {
		groupQueryNamePrefix := getCyclesGroupQueryName(scopeId, cycles[0])
		filteredQueryName := fmt.Sprintf("%s__filtered", groupQueryNamePrefix)
		if len(cycles) > 1 {
			unitedQuery := generateUnitedCyclesQuery(scopeId, cycles)
			groupQueryName := getCyclesGroupQueryName(scopeId, cycles[0])
			unitedQueryName := fmt.Sprintf("%s__united", groupQueryName)
			cq.addItem(unitedQueryName, unitedQuery)
		}
		filteredQuery := generateIntegrityCheckJoinConds(scopeId, cycles)
		cq.addItem(filteredQueryName, filteredQuery)
		shiftCycleGroup(cycles)
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
			leftTableConds = append(leftTableConds, k.GetKeyReference(leftTable))
		}
		var exprs []string
		if len(originalEdge.from.polymorphicExprs) > 0 {
			exprs = append(exprs, originalEdge.from.polymorphicExprs...)
		}
		query = fmt.Sprintf("((%s) IN (%s))", strings.Join(leftTableConds, ", "), query)
		if len(exprs) > 0 {
			query = fmt.Sprintf(
				"((%s) AND (%s) IN (%s))",
				strings.Join(leftTableConds, ", "),
				strings.Join(exprs, "AND"),
				query,
			)
		}

		if scopeEdge.isNullable {
			var nullableChecks []string
			for _, k := range originalEdge.from.keys {
				nullableCheck := fmt.Sprintf(`%s IS NULL`, k.GetKeyReference(originalEdge.from.table))
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

// GetSortedTablesAndDependenciesGraph - returns the sorted tables in topological order and the dependencies graph
// where the key is the table OID and the value is the list of table OIDs that depend on the key table
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

func isPathForScc(path *Path, graph *Graph) bool {
	return graph.scc[path.rootVertex].hasCycle()
}

func generateQuery(
	queryName string, cycle []*Edge, rest []*CondensedEdge, overriddenTables map[toolkit.Oid]string,
) string {
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
		droppedKeysWithAliases = append(
			droppedKeysWithAliases,
			fmt.Sprintf(`%s as "%s__%s__%s"`, k.GetKeyReference(t), t.Schema, t.Name, k.Name),
		)
	}
	selectKeys = append(selectKeys, droppedKeysWithAliases...)

	var initialPathSelectionKeys []string
	for _, k := range cycle[0].from.table.PrimaryKey {
		t := cycle[0].from.table
		pathName := fmt.Sprintf(
			`ARRAY["%s"."%s"."%s"] AS %s__%s__%s__path`,
			t.Schema, t.Name, k,
			t.Schema, t.Name, k,
		)
		initialPathSelectionKeys = append(initialPathSelectionKeys, pathName)
	}

	initialKeys := slices.Clone(selectKeys)
	initialKeys = append(initialKeys, initialPathSelectionKeys...)
	initFromClause := fmt.Sprintf(`FROM "%s"."%s" `, cycle[0].from.table.Schema, cycle[0].from.table.Name)
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
	for _, k := range cycle[0].from.table.PrimaryKey {
		t := cycle[0].from.table
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
	for _, k := range cycle[0].from.table.PrimaryKey {
		t := cycle[0].from.table

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

func generateOverlapQuery(
	scopeId int,
	queryName string, cycle []*Edge, rest []*CondensedEdge, overriddenTables map[toolkit.Oid]string,
	overlap [][]*Edge,
) string {
	var (
		selectKeys                             []string
		initialJoins, recursiveJoins           []string
		initialWhereConds, recursiveWhereConds []string
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
		droppedKeysWithAliases = append(
			droppedKeysWithAliases,
			fmt.Sprintf(`%s as "%s__%s__%s"`, k.GetKeyReference(t), t.Schema, t.Name, k.Name),
		)
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
	initialWhereConds = append(initialWhereConds, generateInClauseForOverlap(scopeId, edges, overlap))

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
	integrityChecks = append(integrityChecks, cycleSubsetConds...)
	initialIntegrityCheck := fmt.Sprintf("(%s) AS valid", strings.Join(integrityChecks, " AND "))
	initialKeys = append(initialKeys, initialIntegrityCheck)
	initialSelect := fmt.Sprintf("SELECT %s", strings.Join(initialKeys, ", "))

	//initialWhereConds = append(initialWhereConds, integrityChecks...)
	initialWhereClause := generateWhereClause(initialWhereConds)
	initialQuery := fmt.Sprintf(`%s %s %s %s`,
		initialSelect, initFromClause, strings.Join(initialJoins, " "), initialWhereClause,
	)

	recursiveIntegrityCheck := fmt.Sprintf("(%s) AS valid", strings.Join(integrityChecks, " AND "))
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

func generateInClauseForOverlap(scopeId int, edges []*Edge, overlap [][]*Edge) string {
	var (
		overlapTables                 []string
		unionQueryParts               []string
		rightTableKeys, leftTableKeys []string
	)

	var shiftedOverlaps [][]*Edge
	for _, oc := range overlap {
		shiftedOverlaps = append(shiftedOverlaps, shiftUntilVertexWillBeFirst(edges[0], oc))
	}

	for _, c := range shiftedOverlaps {
		overlapTables = append(overlapTables, getCycleQueryName(scopeId, c, ""))
	}
	for _, k := range edges[0].from.table.PrimaryKey {
		rightTableKey := fmt.Sprintf(`"%s__%s__%s"`, edges[0].from.table.Schema, edges[0].from.table.Name, k)
		rightTableKeys = append(rightTableKeys, rightTableKey)
		leftTableKey := fmt.Sprintf(`"%s"."%s"."%s"`, edges[0].from.table.Schema, edges[0].from.table.Name, k)
		leftTableKeys = append(leftTableKeys, leftTableKey)
	}
	for _, t := range overlapTables {
		unionQueryParts = append(unionQueryParts, fmt.Sprintf(`SELECT %s FROM "%s"`, strings.Join(rightTableKeys, ", "), t))
	}
	unionQuery := strings.Join(unionQueryParts, " UNION ")

	res := fmt.Sprintf(`(%s) IN (%s)`, strings.Join(leftTableKeys, ", "), unionQuery)
	return res
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
			polymorphicExpr := ""
			if len(e.from.polymorphicExprs) > 0 {
				polymorphicExpr = fmt.Sprintf(" OR NOT (%s)", strings.Join(e.from.polymorphicExprs, " AND "))
			}
			k := fmt.Sprintf(
				`(%s IS NULL OR %s IS NOT NULL%s)`,
				leftTableKey[idx].GetKeyReference(e.from.table),
				rightTableKey[idx].GetKeyReference(e.to.table),
				polymorphicExpr,
			)
			if _, ok := overriddenTables[e.to.table.Oid]; ok {
				if polymorphicExpr != "" {
					panic("IMPLEMENT ME: polymorphic expression for overridden table")
				}
				k = fmt.Sprintf(
					`(%s IS NULL OR "%s"."%s" IS NOT NULL)`,
					leftTableKey[idx].GetKeyReference(e.from.table),
					overriddenTables[e.to.table.Oid],
					rightTableKey[idx].Name,
				)
			}
			keys = append(keys, k)
		}
		res = append(res, fmt.Sprintf("(%s)", strings.Join(keys, " AND ")))
	}
	return
}

func generateUnitedCyclesQuery(scopeId int, cycles [][]*Edge) string {
	var tablesSelection []string
	for _, c := range cycles {
		q1 := fmt.Sprintf(`SELECT * FROM "%s"`, getCycleQueryName(scopeId, c, ""))
		tablesSelection = append(tablesSelection, q1)
		q2 := fmt.Sprintf(`SELECT * FROM "%s"`, getCycleQueryName(scopeId, c, "overlap"))
		tablesSelection = append(tablesSelection, q2)
	}
	res := strings.Join(tablesSelection, " UNION ")
	return res
}

func generateIntegrityCheckJoinConds(scopeId int, cycles [][]*Edge) string {

	var (
		table            = cycles[0][0].from.table
		allPks           []string
		mainTablePks     []string
		unnestSelections []string
		tableName        = getCycleQueryName(scopeId, cycles[0], "")
	)

	if len(cycles) > 1 {
		prefix := getCyclesGroupQueryName(scopeId, cycles[0])
		tableName = fmt.Sprintf("%s__united", prefix)
	}

	for _, t := range getTablesFromCycle(cycles[0]) {
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

func generateAllTablesValidPkSelection(cycles [][]*Edge, scopeId int, forTable *entries.Table) string {

	var unionParts []string

	for _, t := range getTablesFromCycle(cycles[0]) {
		var (
			selectionKeys     []string
			groupId           = getCycleGroupId(cycles[0])
			filteredQueryName = fmt.Sprintf("__s%d__g%s__%s__%s__filtered", scopeId, groupId, t.Schema, t.Name)
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

func getCycleQueryName(scopeId int, cycle []*Edge, postfix string) string {
	// queryName - name of a query in the recursive CTE
	// where:
	//   * s - scope id
	//   * g - group id
	//   * c - cycle id
	//   * postfix with table name
	mainTable := cycle[0].from.table
	groupId := getCycleGroupId(cycle)
	cycleId := getCycleId(cycle)
	res := fmt.Sprintf("__s%d__g%s__c%s__%s__%s", scopeId, groupId, cycleId, mainTable.Schema, mainTable.Name)
	if postfix != "" {
		res = fmt.Sprintf("%s__%s", res, postfix)
	}
	return res
}

func getCyclesGroupQueryName(scopeId int, cycle []*Edge) string {
	// queryName - name of a query in the recursive CTE
	// where:
	//   * s - scope id
	//   * g - group id
	//   * postfix with table name
	mainTable := cycle[0].from.table
	groupId := getCycleGroupId(cycle)
	return getCyclesGroupQueryNameByMainTable(scopeId, groupId, mainTable)
}

func getCyclesGroupQueryNameByMainTable(scopeId int, groupId string, mainTable *entries.Table) string {
	return fmt.Sprintf("__s%d__g%s__%s__%s", scopeId, groupId, mainTable.Schema, mainTable.Name)
}

func shiftCycleGroup(g [][]*Edge) [][]*Edge {
	for idx := range g {
		g[idx] = shiftCycle(g[idx])
	}
	return g
}

func shiftUntilVertexWillBeFirst(v *Edge, c []*Edge) []*Edge {
	//generateInClauseForOverlap
	res := slices.Clone(c)
	for {
		if res[0].from.idx == v.from.idx {
			break
		}
		res = shiftCycle(res)
	}
	return res
}

func getVirtualReferences(vr []*domains.VirtualReference, t *entries.Table) []*domains.Reference {
	idx := slices.IndexFunc(vr, func(r *domains.VirtualReference) bool {
		return r.Schema == t.Schema && r.Name == t.Name
	})
	if idx == -1 {
		return nil
	}
	return vr[idx].References
}

//func getReferencedKeys(r *domains.Reference) (res []string) {
//	for _, ref := range r.Columns {
//		if ref.Name != "" {
//			res = append(res, ref.Name)
//		} else if ref.Expression != "" {
//			res = append(res, ref.Expression)
//		}
//	}
//	return
//}
