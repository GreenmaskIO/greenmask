package tablegraph

import (
	"errors"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/v1/internal/common"
)

var errReferenceTableNotFound = errors.New("reference table not found")

type Graph struct {
	// tables - the tables that are part of the graph.
	//
	// The index of the table in the slice is the index of the table in the graph.
	// This index used in TableLink to reference the table as well.
	tables []common.Table
	// graph - the oriented graph representation of the DB tables.
	graph [][]*Edge
}

// NewGraph - creates a new Graph instance.
//
// It builds a graph based on the introspected tables and their references.
func NewGraph(tables []common.Table) (Graph, error) {
	var edgeIdSequence int
	graph := make([][]*Edge, len(tables))
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
				referenceTableIdx,
				reference.IsNullable,
				from,
				to,
			)
			graph[tableIdx] = append(
				graph[tableIdx],
				edge,
			)
			edgeIdSequence++
		}
	}
	return Graph{
		tables: tables,
		graph:  graph,
	}, nil
}
