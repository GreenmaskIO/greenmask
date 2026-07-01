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

// Package subsetbuilder implements core.SubsetBuilder by adapting the
// query-building logic from pkg/common/subset to operate directly on the
// DependencyGraphResult produced by GraphBuilder.
//
// The condensation graph (SCCs and their edges) is computed once by GraphBuilder
// and carried in SubsetBuilderInput.DependencyGraph.  This package consumes it
// as-is and only performs the subset-path search and SQL generation, avoiding
// the duplicate tablegraph/condensationgraph construction that would occur if
// pkg/common/subset.NewSubset were called directly.
//
// Query building is split into two strategies matching pkg/common/subset:
//   - dagQueryBuilder  — acyclic SCCs (one member each); see dag_query_builder.go
//   - cyclesQueryBuilder — cyclic SCCs (multiple members); see cycles_query_builder.go
package subsetbuilder

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var _ core.SubsetBuilder = (*SubsetBuilder)(nil)

// sccQueryBuilder is the internal interface dispatched by buildSubsetQuery.
// It mirrors the queryBuilder interface in pkg/common/subset/subset.go.
// The map key is ObjectID; for DAG SCCs it contains a single entry, for cyclic
// SCCs it may contain one entry per table in the cycle.
type sccQueryBuilder interface {
	build() (map[core.ObjectID]string, error)
}

// SubsetBuilder implements core.SubsetBuilder.
type SubsetBuilder struct {
	dialect Dialect
	// tableKind is the engine-specific object kind under which the introspection
	// result stores table objects (e.g. the engine's "mysql.table" kind).
	tableKind core.ObjectKind
}

// New creates a SubsetBuilder for the given SQL dialect. tableKind is the object
// kind under which the introspection result stores tables.
// Table configs carrying subset_conds are passed per-call via SubsetBuilderInput.
func New(dialect Dialect, tableKind core.ObjectKind) *SubsetBuilder {
	return &SubsetBuilder{dialect: dialect, tableKind: tableKind}
}

// BuildSubset generates WHERE-clause queries for every table reachable from a
// subset-conditioned table in the dependency graph.
//
// It uses in.DependencyGraph (already computed by GraphBuilder) directly —
// no graph is rebuilt here.
func (b *SubsetBuilder) BuildSubset(
	_ context.Context,
	in core.SubsetBuilderInput,
) (core.SubsetResult, error) {
	subsetConds := buildSubsetCondsMap(in.Introspection, in.TableConfigs, b.tableKind)
	subgraphs := searchSubsetGraphs(in.DependencyGraph, subsetConds)

	subsetMap := make(map[core.ObjectID]string)
	for _, sg := range subgraphs {
		queries, err := buildSubsetQuery(sg, in.DependencyGraph, subsetConds, b.dialect)
		if err != nil {
			return core.SubsetResult{}, fmt.Errorf("build query for SCC %d: %w", sg.rootSCCID, err)
		}
		for oid, q := range queries {
			if q != "" {
				subsetMap[oid] = q
			}
		}
	}

	return core.SubsetResult{SubsetMap: subsetMap}, nil
}

// buildSubsetQuery dispatches to dagQueryBuilder or cyclesQueryBuilder based on
// whether the root SCC is a single-table DAG node or a multi-table cycle group.
// Mirrors Subset.buildQueryForSCC in pkg/common/subset/subset.go.
func buildSubsetQuery(
	sg *sccSubgraph,
	dg core.DependencyGraphResult,
	subsetConds map[core.ObjectID][]string,
	dialect Dialect,
) (map[core.ObjectID]string, error) {
	rootNode := dg.CondensedGraph.Nodes[sg.rootSCCID]
	var b sccQueryBuilder
	if len(rootNode.Members) > 1 {
		b = newCyclesQueryBuilder(sg, dg, subsetConds, dialect)
	} else {
		b = newDAGQueryBuilder(sg, dg, subsetConds, dialect)
	}
	return b.build()
}

// buildSubsetCondsMap builds a map from ObjectID to its subset conditions by
// matching tableConfigs (schema+name) against the introspection objects.
func buildSubsetCondsMap(
	introspection core.IntrospectionResult,
	tableConfigs []core.TableConfig,
	tableKind core.ObjectKind,
) map[core.ObjectID][]string {
	nameToID := make(map[string]core.ObjectID)
	for _, obj := range introspection.KindsMap[tableKind] {
		if tbl, err := tableFromPayload(obj.Payload); err == nil {
			nameToID[tbl.Schema+"."+tbl.Name] = obj.ID
		}
	}
	result := make(map[core.ObjectID][]string)
	for _, tc := range tableConfigs {
		if len(tc.SubsetConds) == 0 {
			continue
		}
		if oid, ok := nameToID[tc.Schema+"."+tc.Name]; ok {
			result[oid] = tc.SubsetConds
		}
	}
	return result
}

// ── SCC sub-graph ────────────────────────────────────────────────────────────

// sccSubgraph is the per-root sub-graph of condensed SCCs that are on a path
// leading to at least one subset-conditioned table.  It mirrors the role of
// subsetGraph in pkg/common/subset/subset_graph.go but uses SCCID keys and
// the DependencyGraphResult's SCCEdge type.
type sccSubgraph struct {
	rootSCCID core.SCCID
	// graph: SCCID → outgoing SCCEdges within this sub-graph.
	// A nil slice value means the SCC is present but has no outgoing edges.
	graph map[core.SCCID][]core.SCCEdge
}

func newSCCSubgraph(rootSCCID core.SCCID) *sccSubgraph {
	return &sccSubgraph{
		rootSCCID: rootSCCID,
		graph:     make(map[core.SCCID][]core.SCCEdge),
	}
}

func (sg *sccSubgraph) addVertex(sccID core.SCCID) {
	if _, ok := sg.graph[sccID]; !ok {
		sg.graph[sccID] = nil
	}
}

func (sg *sccSubgraph) addEdge(e core.SCCEdge) {
	sg.graph[e.From] = append(sg.graph[e.From], e)
	if _, ok := sg.graph[e.To]; !ok {
		sg.graph[e.To] = nil
	}
}

func (sg *sccSubgraph) addPath(path []core.SCCEdge) {
	for _, e := range path {
		sg.addEdge(e)
	}
}

// ── Sub-graph search ─────────────────────────────────────────────────────────

// sccHasSubsetConds reports whether any member of sccID has user-defined
// subset conditions.
func sccHasSubsetConds(
	sccID core.SCCID,
	dg core.DependencyGraphResult,
	subsetConds map[core.ObjectID][]string,
) bool {
	node, ok := dg.CondensedGraph.Nodes[sccID]
	if !ok {
		return false
	}
	for _, memberID := range node.Members {
		if _, ok := subsetConds[memberID]; ok {
			return true
		}
	}
	return false
}

// searchSubsetGraphs builds one sccSubgraph per SCC that acts as a "root" —
// i.e. has at least one path to a subset-conditioned SCC.  This mirrors
// Subset.searchTablesGraph in pkg/common/subset/subset.go.
func searchSubsetGraphs(
	dg core.DependencyGraphResult,
	subsetConds map[core.ObjectID][]string,
) []*sccSubgraph {
	sccIDs := sortedSCCIDs(dg.CondensedGraph)

	var result []*sccSubgraph
	for _, sccID := range sccIDs {
		sg := newSCCSubgraph(sccID)
		if sccHasSubsetConds(sccID, dg, subsetConds) {
			sg.addVertex(sccID)
		}
		var from []core.SCCEdge
		searchDFS(sccID, dg, subsetConds, &from, sg)
		if len(sg.graph) > 0 {
			result = append(result, sg)
		}
	}
	return result
}

// searchDFS is the recursive DFS used by searchSubsetGraphs.  It mirrors
// Subset.searchTablesGraphDFS in pkg/common/subset/subset.go.
func searchDFS(
	v core.SCCID,
	dg core.DependencyGraphResult,
	subsetConds map[core.ObjectID][]string,
	from *[]core.SCCEdge,
	sg *sccSubgraph,
) {
	edges := dg.CondensedGraph.Edges[v]
	sorted := make([]core.SCCEdge, len(edges))
	copy(sorted, edges)
	slices.SortFunc(sorted, func(a, b core.SCCEdge) int { return cmp.Compare(a.To, b.To) })

	for _, edge := range sorted {
		*from = append(*from, edge)
		if sccHasSubsetConds(edge.To, dg, subsetConds) {
			sg.addPath(*from)
			*from = (*from)[:0]
		}
		searchDFS(edge.To, dg, subsetConds, from, sg)
		if len(*from) > 0 {
			*from = (*from)[:len(*from)-1]
		}
	}
}

// ── Shared utilities ──────────────────────────────────────────────────────────

// tableFromPayload extracts a core.Table from an ObjectNode or Object
// payload.  Accepts Table, *Table, or any type implementing ToCommonTable().
func tableFromPayload(payload any) (core.Table, error) {
	switch p := payload.(type) {
	case core.Table:
		return p, nil
	case *core.Table:
		if p == nil {
			return core.Table{}, fmt.Errorf("nil *Table payload")
		}
		return *p, nil
	case interface{ ToCommonTable() core.Table }:
		return p.ToCommonTable(), nil
	default:
		return core.Table{}, fmt.Errorf("unsupported payload type %T", p)
	}
}

func sortedSCCIDs(cg core.CondensedGraph) []core.SCCID {
	ids := make([]core.SCCID, 0, len(cg.Nodes))
	for id := range cg.Nodes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func sortedSCCIDsFromMap(m map[core.SCCID][]core.SCCEdge) []core.SCCID {
	ids := make([]core.SCCID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
