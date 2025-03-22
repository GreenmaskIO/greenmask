package tablegraph

import (
	"errors"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/v1/internal/common"
)

var errReferenceTableNotFound = errors.New("reference table not found")

type Graph struct {
	// Vertexes - the Vertexes that are part of the Graph. Basically they are the tables in the DB.
	//
	// The index of the table in the slice is the index of the table in the Graph.
	// This index used in TableLink to reference the table as well.
	Vertexes []common.Table
	// Graph - the oriented Graph representation of the DB Vertexes.
	Graph [][]Edge
	// TransposedGraph - the transposed Graph representation of the DB Vertexes.
	TransposedGraph [][]Edge
}

// NewGraph - creates a new Graph instance.
//
// It builds a Graph based on the introspected Vertexes and their references.
func NewGraph(tables []common.Table) (Graph, error) {
	var edgeIdSequence int
	graph := make([][]Edge, len(tables))
	transposedGraph := make([][]Edge, len(tables))
	for tableIdx, table := range tables {
		for _, reference := range table.References {
			referenceTableIdx := slices.IndexFunc(tables, func(t common.Table) bool {
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
