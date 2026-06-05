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

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/subset/condensationgraph"
)

// buildCondensedGraph converts the condensation graph into the result's condensed
// graph (one node per strongly connected component) and the object->SCC index.
func (t *translator) buildCondensedGraph(
	cg condensationgraph.Graph,
) (commonmodels.CondensedGraph, map[commonmodels.ObjectID]commonmodels.SCCID) {
	nodes := make(map[commonmodels.SCCID]commonmodels.SCCNode, len(cg.SCC))
	objectToSCC := make(map[commonmodels.ObjectID]commonmodels.SCCID)

	for _, scc := range cg.SCC {
		sccID := commonmodels.SCCID(scc.ID())

		// SCCGraph is keyed by the vertex positions that make up the component
		// (every member has an entry, with possibly-nil intra-SCC edges).
		members := make([]commonmodels.ObjectID, 0, len(scc.SCCGraph))
		for pos := range scc.SCCGraph {
			oid := t.idAt(pos)
			members = append(members, oid)
			objectToSCC[oid] = sccID
		}
		// SCCGraph iterates a map; sort members for a stable result.
		slices.Sort(members)

		node := commonmodels.SCCNode{
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

	edges := make(map[commonmodels.SCCID][]commonmodels.SCCEdge)
	for fromIdx := range cg.Graph {
		fromSCC := commonmodels.SCCID(fromIdx)
		// Collapse parallel condensed edges between the same SCC pair into a single
		// SCCEdge that carries all the underlying object edges that caused it.
		linksByTo := make(map[commonmodels.SCCID][]commonmodels.ObjectEdge)
		for _, e := range cg.Graph[fromIdx] {
			toSCC := commonmodels.SCCID(e.To().TableID())
			linksByTo[toSCC] = append(linksByTo[toSCC], t.objectEdge(e.OriginalEdge()))
		}

		toSCCs := make([]commonmodels.SCCID, 0, len(linksByTo))
		for toSCC := range linksByTo {
			toSCCs = append(toSCCs, toSCC)
		}
		slices.Sort(toSCCs)
		for _, toSCC := range toSCCs {
			edges[fromSCC] = append(edges[fromSCC], commonmodels.SCCEdge{
				From:  fromSCC,
				To:    toSCC,
				Links: linksByTo[toSCC],
			})
		}
	}

	return commonmodels.CondensedGraph{Nodes: nodes, Edges: edges}, objectToSCC
}

// sccSubgraph builds the object graph internal to a single SCC (the edges that
// stay within the component). For acyclic single-table SCCs this is just the node
// with no edges.
func (t *translator) sccSubgraph(scc condensationgraph.SCC) commonmodels.ObjectGraph {
	nodes := make(map[commonmodels.ObjectID]commonmodels.ObjectNode, len(scc.SCCGraph))
	for pos := range scc.SCCGraph {
		node := t.nodeAt(pos)
		nodes[node.ID] = node
	}

	edges := make(map[commonmodels.ObjectID][]commonmodels.ObjectEdge)
	for _, intraEdges := range scc.SCCGraph {
		for _, e := range intraEdges {
			oe := t.objectEdge(e)
			edges[oe.From] = append(edges[oe.From], oe)
		}
	}

	return commonmodels.ObjectGraph{Nodes: nodes, Edges: edges}
}
