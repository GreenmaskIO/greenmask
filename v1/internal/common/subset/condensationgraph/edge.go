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

package condensationgraph

import "github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"

// Edge - represents an edge in the condensation Graph.
//
// It encapsulates the original edge from the table Graph and the condensed vertexes.
type Edge struct {
	// id - unique identifier of the edge.
	id int
	// from - the left part of the edge.
	from Link
	// to - the right part of the edge.
	to Link
	// originalEdge - the original edge from the table Graph.
	originalEdge tablegraph.Edge
}

// NewEdge - creates a new Edge instance.
func NewEdge(id int, from, to Link, originalEdge tablegraph.Edge) Edge {
	return Edge{
		id:           id,
		from:         from,
		to:           to,
		originalEdge: originalEdge,
	}
}

func (e Edge) ID() int {
	return e.id
}

func (e Edge) From() Link {
	return e.from
}

func (e Edge) To() Link {
	return e.to
}

func (e Edge) OriginalEdge() tablegraph.Edge {
	return e.originalEdge
}
