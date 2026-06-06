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

// Package subsetbuilder implements interfaces.SubsetBuilder by adapting the
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

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/subset"
)

var _ interfaces.SubsetBuilder = (*SubsetBuilder)(nil)

// sccQueryBuilder is the internal interface dispatched by buildSubsetQuery.
// It mirrors the queryBuilder interface in pkg/common/subset/subset.go.
// The map key is ObjectID; for DAG SCCs it contains a single entry, for cyclic
// SCCs it may contain one entry per table in the cycle.
type sccQueryBuilder interface {
	build() (map[commonmodels.ObjectID]string, error)
}

// SubsetBuilder implements interfaces.SubsetBuilder.
type SubsetBuilder struct {
	dialect subset.Dialect
}

// New creates a SubsetBuilder for the given SQL dialect.
// Table configs carrying subset_conds are passed per-call via SubsetBuilderInput.
func New(dialect subset.Dialect) *SubsetBuilder {
	return &SubsetBuilder{dialect: dialect}
}

// BuildSubset generates WHERE-clause queries for every table reachable from a
// subset-conditioned table in the dependency graph.
//
// It uses in.DependencyGraph (already computed by GraphBuilder) directly —
// no graph is rebuilt here.
func (b *SubsetBuilder) BuildSubset(
	_ context.Context,
	in commonmodels.SubsetBuilderInput,
) (commonmodels.SubsetResult, error) {
	subsetConds := buildSubsetCondsMap(in.Introspection, in.TableConfigs)
	subgraphs := searchSubsetGraphs(in.DependencyGraph, subsetConds)

	subsetMap := make(map[commonmodels.ObjectID]string)
	for _, sg := range subgraphs {
		queries, err := buildSubsetQuery(sg, in.DependencyGraph, subsetConds, b.dialect)
		if err != nil {
			return commonmodels.SubsetResult{}, fmt.Errorf("build query for SCC %d: %w", sg.rootSCCID, err)
		}
		for oid, q := range queries {
			if q != "" {
				subsetMap[oid] = q
			}
		}
	}

	return commonmodels.SubsetResult{SubsetMap: subsetMap}, nil
}

// buildSubsetQuery dispatches to dagQueryBuilder or cyclesQueryBuilder based on
// whether the root SCC is a single-table DAG node or a multi-table cycle group.
// Mirrors Subset.buildQueryForSCC in pkg/common/subset/subset.go.
func buildSubsetQuery(
	sg *sccSubgraph,
	dg commonmodels.DependencyGraphResult,
	subsetConds map[commonmodels.ObjectID][]string,
	dialect subset.Dialect,
) (map[commonmodels.ObjectID]string, error) {
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
	introspection commonmodels.IntrospectionResult,
	tableConfigs []commonmodels.TableConfig,
) map[commonmodels.ObjectID][]string {
	nameToID := make(map[string]commonmodels.ObjectID)
	for _, obj := range introspection.KindsMap[commonmodels.ObjectKindTable] {
		if tbl, err := tableFromPayload(obj.Payload); err == nil {
			nameToID[tbl.Schema+"."+tbl.Name] = obj.ID
		}
	}
	result := make(map[commonmodels.ObjectID][]string)
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
	rootSCCID commonmodels.SCCID
	// graph: SCCID → outgoing SCCEdges within this sub-graph.
	// A nil slice value means the SCC is present but has no outgoing edges.
	graph map[commonmodels.SCCID][]commonmodels.SCCEdge
}

func newSCCSubgraph(rootSCCID commonmodels.SCCID) *sccSubgraph {
	return &sccSubgraph{
		rootSCCID: rootSCCID,
		graph:     make(map[commonmodels.SCCID][]commonmodels.SCCEdge),
	}
}

func (sg *sccSubgraph) addVertex(sccID commonmodels.SCCID) {
	if _, ok := sg.graph[sccID]; !ok {
		sg.graph[sccID] = nil
	}
}

func (sg *sccSubgraph) addEdge(e commonmodels.SCCEdge) {
	sg.graph[e.From] = append(sg.graph[e.From], e)
	if _, ok := sg.graph[e.To]; !ok {
		sg.graph[e.To] = nil
	}
}

func (sg *sccSubgraph) addPath(path []commonmodels.SCCEdge) {
	for _, e := range path {
		sg.addEdge(e)
	}
}

// ── Sub-graph search ─────────────────────────────────────────────────────────

// sccHasSubsetConds reports whether any member of sccID has user-defined
// subset conditions.
func sccHasSubsetConds(
	sccID commonmodels.SCCID,
	dg commonmodels.DependencyGraphResult,
	subsetConds map[commonmodels.ObjectID][]string,
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
	dg commonmodels.DependencyGraphResult,
	subsetConds map[commonmodels.ObjectID][]string,
) []*sccSubgraph {
	sccIDs := sortedSCCIDs(dg.CondensedGraph)

	var result []*sccSubgraph
	for _, sccID := range sccIDs {
		sg := newSCCSubgraph(sccID)
		if sccHasSubsetConds(sccID, dg, subsetConds) {
			sg.addVertex(sccID)
		}
		var from []commonmodels.SCCEdge
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
	v commonmodels.SCCID,
	dg commonmodels.DependencyGraphResult,
	subsetConds map[commonmodels.ObjectID][]string,
	from *[]commonmodels.SCCEdge,
	sg *sccSubgraph,
) {
	edges := dg.CondensedGraph.Edges[v]
	sorted := make([]commonmodels.SCCEdge, len(edges))
	copy(sorted, edges)
	slices.SortFunc(sorted, func(a, b commonmodels.SCCEdge) int { return cmp.Compare(a.To, b.To) })

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

// tableFromPayload extracts a commonmodels.Table from an ObjectNode or Object
// payload.  Accepts Table, *Table, or any type implementing ToCommonTable().
func tableFromPayload(payload any) (commonmodels.Table, error) {
	switch p := payload.(type) {
	case commonmodels.Table:
		return p, nil
	case *commonmodels.Table:
		if p == nil {
			return commonmodels.Table{}, fmt.Errorf("nil *Table payload")
		}
		return *p, nil
	case interface{ ToCommonTable() commonmodels.Table }:
		return p.ToCommonTable(), nil
	default:
		return commonmodels.Table{}, fmt.Errorf("unsupported payload type %T", p)
	}
}

func sortedSCCIDs(cg commonmodels.CondensedGraph) []commonmodels.SCCID {
	ids := make([]commonmodels.SCCID, 0, len(cg.Nodes))
	for id := range cg.Nodes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func sortedSCCIDsFromMap(m map[commonmodels.SCCID][]commonmodels.SCCEdge) []commonmodels.SCCID {
	ids := make([]commonmodels.SCCID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
