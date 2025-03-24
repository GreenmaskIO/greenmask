package condensationgraph

import (
	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/cyclesgraph"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

// SCC - represents a strongly connected ssc in the graph. It may contain one vertex (table) with no cycles
// or multiple vertexes (vertexes) with cycles.
type SCC struct {
	// id - the unique identifier of the ssc
	id int
	// cyclesGraph - graph of cycles in the ssc and their links.
	//
	// This graph can be used to generate correct SQL queries to check the integrity of the group of cycles.
	cyclesGraph cyclesgraph.Graph
	// sccGraph - contains the mapping of the vertexes in the ssc to the edges in the original graph
	// if the ssc contains one vertex and no edges, then there is only one vertex with no cycles.
	sccGraph map[int][]tablegraph.Edge
	// vertexes - the vertexes in the ssc.
	vertexes map[int]common.Table
}

// NewSCC - creates a new SCC instance.
//
// It receives the unique identifier of the ssc, the graph of the ssc (it contains a table graph edges)
// and the vertexes that are part of the ssc.
//
// sccGraph - is a graph of table id to the edges that are connected to the table.
// vertexes - the vertexes that are part of the ssc. You can use an index from the sccGraph to get the table
// instance.
func NewSCC(
	id int,
	sccGraph map[int][]tablegraph.Edge,
	vertexes map[int]common.Table,
) SCC {
	cyclesGraph := cyclesgraph.NewGraph(sccGraph)
	c := SCC{
		id:          id,
		cyclesGraph: cyclesGraph,
		sccGraph:    sccGraph,
		vertexes:    vertexes,
	}

	return c
}

// HasSubsetConditions - returns true if at least one condensed vertex has subset condition.
func (c *SCC) HasSubsetConditions() bool {
	for _, t := range c.vertexes {
		if t.HasSubsetConditions() {
			return true
		}
	}
	return false
}

// HasCycle - returns true if the ssc has cycles.
func (c *SCC) HasCycle() bool {
	return c.cyclesGraph.HasCycle()
}

// CyclesGroupCount - returns the count of the cycles group in the ssc.
func (c *SCC) CyclesGroupCount() int {
	return c.cyclesGraph.CyclesGroupCount()
}

// Vertexes - returns the vertexes in the ssc.
func (c *SCC) Vertexes() []common.Table {
	var vertexes []common.Table
	for _, table := range c.vertexes {
		vertexes = append(vertexes, table)
	}
	return vertexes
}
