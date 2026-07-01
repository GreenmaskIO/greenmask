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

package graphbuilder

import (
	"slices"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/subset/condensationgraph"
)

// buildCondensedGraph converts the condensation graph into the result's condensed
// graph (one node per strongly connected component) and the object->SCC index.
func (t *translator) buildCondensedGraph(
	cg condensationgraph.Graph,
) (core.CondensedGraph, map[core.ObjectID]core.SCCID) {
	nodes := make(map[core.SCCID]core.SCCNode, len(cg.SCC))
	objectToSCC := make(map[core.ObjectID]core.SCCID)

	for _, scc := range cg.SCC {
		sccID := core.SCCID(scc.ID())

		// SCCGraph is keyed by the vertex positions that make up the component
		// (every member has an entry, with possibly-nil intra-SCC edges).
		members := make([]core.ObjectID, 0, len(scc.SCCGraph))
		for pos := range scc.SCCGraph {
			oid := t.idAt(pos)
			members = append(members, oid)
			objectToSCC[oid] = sccID
		}
		// SCCGraph iterates a map; sort members for a stable result.
		slices.Sort(members)

		node := core.SCCNode{
			ID:       sccID,
			Members:  members,
			Subgraph: t.sccSubgraph(scc),
		}
		if scc.HasCycle() {
			cycles := t.cycleGraph(scc.CyclesGraph)
			node.Cycles = &cycles
		}
		nodes[sccID] = node
	}

	edges := make(map[core.SCCID][]core.SCCEdge)
	for fromIdx := range cg.Graph {
		fromSCC := core.SCCID(fromIdx)
		// Collapse parallel condensed edges between the same SCC pair into a single
		// SCCEdge that carries all the underlying object edges that caused it.
		linksByTo := make(map[core.SCCID][]core.ObjectEdge)
		for _, e := range cg.Graph[fromIdx] {
			toSCC := core.SCCID(e.To().TableID())
			linksByTo[toSCC] = append(linksByTo[toSCC], t.objectEdge(e.OriginalEdge()))
		}

		toSCCs := make([]core.SCCID, 0, len(linksByTo))
		for toSCC := range linksByTo {
			toSCCs = append(toSCCs, toSCC)
		}
		slices.Sort(toSCCs)
		for _, toSCC := range toSCCs {
			edges[fromSCC] = append(edges[fromSCC], core.SCCEdge{
				From:  fromSCC,
				To:    toSCC,
				Links: linksByTo[toSCC],
			})
		}
	}

	return core.CondensedGraph{Nodes: nodes, Edges: edges}, objectToSCC
}

// sccSubgraph builds the object graph internal to a single SCC (the edges that
// stay within the component). For acyclic single-table SCCs this is just the node
// with no edges.
func (t *translator) sccSubgraph(scc condensationgraph.SCC) core.ObjectGraph {
	nodes := make(map[core.ObjectID]core.ObjectNode, len(scc.SCCGraph))
	for pos := range scc.SCCGraph {
		node := t.nodeAt(pos)
		nodes[node.ID] = node
	}

	edges := make(map[core.ObjectID][]core.ObjectEdge)
	for _, intraEdges := range scc.SCCGraph {
		for _, e := range intraEdges {
			oe := t.objectEdge(e)
			edges[oe.From] = append(edges[oe.From], oe)
		}
	}

	return core.ObjectGraph{Nodes: nodes, Edges: edges}
}
