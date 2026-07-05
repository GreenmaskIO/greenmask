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
	"maps"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/subset/tablegraph"
)

// buildObjectGraph converts the directed table graph into the object graph: one
// node per table object and one edge per foreign-key reference (child -> parent).
func (t *translator) buildObjectGraph(tg tablegraph.Graph) core.ObjectGraph {
	// Every table object is a node; clone so the result does not alias translator
	// state.
	nodes := maps.Clone(t.nodes)

	edges := make(map[core.ObjectID][]core.ObjectEdge)
	for vertexPos := range tg.Graph {
		for _, e := range tg.Graph[vertexPos] {
			oe := t.objectEdge(e)
			edges[oe.From] = append(edges[oe.From], oe)
		}
	}

	return core.ObjectGraph{
		Nodes: nodes,
		Edges: edges,
	}
}
