package condensationgraph

import (
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/cyclesgraph"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

// SCC - represents a strongly connected SCC in the Graph. It may contain one vertex (table) with no cycles
// or multiple vertexes (vertexes) with cycles.
type SCC struct {
	// id - the unique identifier of the SCC
	id int
	// CyclesGraph - Graph of cycles in the SCC and their links.
	//
	// This Graph can be used to generate correct SQL queries to check the integrity of the group of cycles.
	CyclesGraph cyclesgraph.Graph
	// SCCGraph - contains the mapping of the vertexes in the SCC to the edges in the original Graph
	// if the SCC contains one vertex and no edges, then there is only one vertex with no cycles.
	// So it's a sub-graph for SCC.
	SCCGraph map[int][]tablegraph.Edge
	// vertexes - the vertexes in the SCC.
	vertexes map[int]commonmodels.Table
}

// NewSCC - creates a new SCC instance.
//
// It receives the unique identifier of the SCC, the Graph of the SCC (it contains a table Graph edges)
// and the vertexes that are part of the SCC.
//
// SCCGraph - is a Graph of table id to the edges that are connected to the table.
// vertexes - the vertexes that are part of the SCC. You can use an index from the SCCGraph to get the table
// instance.
func NewSCC(
	id int,
	sccGraph map[int][]tablegraph.Edge,
	vertexes map[int]commonmodels.Table,
) SCC {
	cyclesGraph := cyclesgraph.NewGraph(sccGraph)
	c := SCC{
		id:          id,
		CyclesGraph: cyclesGraph,
		SCCGraph:    sccGraph,
		vertexes:    vertexes,
	}

	return c
}

// HasSubsetConditions - returns true if at least one condensed vertex has subset condition.
func (c SCC) HasSubsetConditions() bool {
	for _, t := range c.vertexes {
		if t.HasSubsetConditions() {
			return true
		}
	}
	return false
}

// ID - returns ID of SCC.
func (c SCC) ID() int {
	return c.id
}

// HasCycle - returns true if the SCC has cycles.
func (c SCC) HasCycle() bool {
	return c.CyclesGraph.HasCycle()
}

// CyclesGroupCount - returns the count of the cycles group in the SCC.
func (c SCC) CyclesGroupCount() int {
	return c.CyclesGraph.CyclesGroupCount()
}

// Vertexes - returns the vertexes in the SCC.
// Should we cache it?
func (c SCC) Vertexes() []commonmodels.Table {
	var vertexes []commonmodels.Table
	for _, table := range c.vertexes {
		vertexes = append(vertexes, table)
	}
	return vertexes
}
