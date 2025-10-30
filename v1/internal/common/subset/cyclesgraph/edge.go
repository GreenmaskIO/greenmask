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

package cyclesgraph

import commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"

// Edge - represents an edge in the Graph in Cycles.
//
// It connects two Cycles via commonmodels vertexes. For example, we have two Cycles 1->2->3 and 2->3->4.
// The commonmodels vertexes are 2 and 3.
// The from value will be 1_2_3 and the to value will be 2_3_4.
type Edge struct {
	// id - the unique identifier of the edge.
	id int
	// from - the from cycle identifier.
	from string
	// to - the to cycle identifier.
	to string
	// commonVertexes - the commonmodels vertexes that can be used to join the Cycles.
	commonVertexes []commonmodels.Table
}

// NewEdge - creates a new Edge instance.
func NewEdge(id int, from, to string, tables []commonmodels.Table) Edge {
	if len(tables) == 0 {
		panic("empty commonVertexes provided for cycle edge")
	}
	return Edge{
		id:             id,
		from:           from,
		to:             to,
		commonVertexes: tables,
	}
}
