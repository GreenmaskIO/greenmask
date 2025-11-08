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

package tablegraph

import (
	"errors"
	"fmt"
	"slices"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

var errReferenceTableNotFound = errors.New("reference table not found")

type Graph struct {
	// Vertexes - the Vertexes that are part of the Graph. Basically they are the tables in the DB.
	//
	// The index of the table in the slice is the index of the table in the Graph.
	// This index used in TableLink to reference the table as well.
	Vertexes []commonmodels.Table
	// Graph - the oriented Graph representation of the DB Vertexes.
	Graph [][]Edge
	// TransposedGraph - the transposed Graph representation of the DB Vertexes.
	TransposedGraph [][]Edge
}

// NewGraph - creates a new Graph instance.
//
// It builds a Graph based on the introspected Vertexes and their references.
func NewGraph(tables []commonmodels.Table) (Graph, error) {
	var edgeIdSequence int
	graph := make([][]Edge, len(tables))
	transposedGraph := make([][]Edge, len(tables))
	for tableIdx, table := range tables {
		for _, reference := range table.References {
			referenceTableIdx := slices.IndexFunc(tables, func(t commonmodels.Table) bool {
				return t.Name == reference.ReferencedName && t.Schema == reference.ReferencedSchema
			})

			if referenceTableIdx == -1 {
				return Graph{}, fmt.Errorf(
					"reference search %s.%s: %w",
					table.Schema, table.Name, errReferenceTableNotFound,
				)
			}

			from := NewTableLink(
				tableIdx,
				table,
				NewKeysByColumn(reference.Keys),
				nil,
			)
			to := NewTableLink(
				referenceTableIdx,
				tables[referenceTableIdx],
				NewKeysByColumn(tables[referenceTableIdx].PrimaryKey),
				nil,
			)

			edge := NewEdge(
				edgeIdSequence,
				reference.IsNullable,
				from,
				to,
			)
			graph[tableIdx] = append(
				graph[tableIdx],
				edge,
			)

			// Transpose the edge

			transposedFrom := NewTableLink(
				referenceTableIdx,
				tables[referenceTableIdx],
				NewKeysByColumn(tables[referenceTableIdx].PrimaryKey),
				nil,
			)

			transposedTo := NewTableLink(
				tableIdx,
				table,
				NewKeysByColumn(reference.Keys),
				nil,
			)

			transposedEdge := NewEdge(
				edgeIdSequence,
				reference.IsNullable,
				transposedFrom,
				transposedTo,
			)

			transposedGraph[referenceTableIdx] = append(
				transposedGraph[referenceTableIdx],
				transposedEdge,
			)
			edgeIdSequence++
		}
	}
	return Graph{
		Vertexes:        tables,
		Graph:           graph,
		TransposedGraph: transposedGraph,
	}, nil
}
