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
	"sort"
	"strconv"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/subset/cyclesgraph"
)

// cycleGraph converts an SCC's cycle graph into the result model: the list of
// cycles, the cycle groups (cycles sharing the same vertex set) and the graph
// between groups (which groups share vertexes and can be joined for integrity).
func (t *translator) cycleGraph(cg cyclesgraph.Graph) core.CycleGraph {
	res := core.CycleGraph{
		Cycles:     make([]core.Cycle, 0, len(cg.Cycles)),
		Groups:     make(map[core.CycleGroupID]core.CycleGroup),
		GroupGraph: make(map[core.CycleGroupID][]core.CycleGroupEdge),
	}

	// Cycles keep their slice order so the CycleIndex values referenced by groups
	// remain valid.
	for i, cycle := range cg.Cycles {
		edges := make([]core.ObjectEdge, 0, len(cycle))
		for _, e := range cycle {
			edges = append(edges, t.objectEdge(e))
		}
		res.Cycles = append(res.Cycles, core.Cycle{
			ID:    core.CycleID(strconv.Itoa(i)),
			Edges: edges,
		})
	}

	for _, groupID := range sortedStringKeys(cg.GroupedCycles) {
		cycleIdxs := cg.GroupedCycles[groupID]
		indexes := make([]core.CycleIndex, 0, len(cycleIdxs))
		var members []core.ObjectID
		seen := make(map[core.ObjectID]struct{})
		for _, ci := range cycleIdxs {
			indexes = append(indexes, core.CycleIndex(ci))
			for _, e := range cg.Cycles[ci] {
				oid := t.idAt(e.To().TableID())
				if _, ok := seen[oid]; ok {
					continue
				}
				seen[oid] = struct{}{}
				members = append(members, oid)
			}
		}
		res.Groups[core.CycleGroupID(groupID)] = core.CycleGroup{
			ID:      core.CycleGroupID(groupID),
			Cycles:  indexes,
			Members: members,
		}
	}

	for _, fromGroup := range sortedStringKeys(cg.Graph) {
		for _, e := range cg.Graph[fromGroup] {
			// CommonVertexes are reported as tables (not positions); resolve them to
			// ObjectIDs by their fully-qualified name.
			shared := make([]core.ObjectID, 0, len(e.CommonVertexes()))
			for _, tbl := range e.CommonVertexes() {
				shared = append(shared, t.objectIDByName[tbl.FullTableName()])
			}
			// Links (the specific object edges between the groups) are not set:
			// the source cycle graph only records the shared vertexes, not the
			// edges that join the groups, so SharedObjects is the available signal.
			res.GroupGraph[core.CycleGroupID(fromGroup)] = append(
				res.GroupGraph[core.CycleGroupID(fromGroup)],
				core.CycleGroupEdge{
					From:          core.CycleGroupID(e.From()),
					To:            core.CycleGroupID(e.To()),
					SharedObjects: shared,
				},
			)
		}
	}

	return res
}

// sortedStringKeys returns the keys of a string-keyed map in deterministic order.
func sortedStringKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
