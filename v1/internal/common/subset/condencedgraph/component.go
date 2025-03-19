package condencedgraph

import (
	"errors"
	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/cyclesgraph"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

var (
	errComponentHasMoreThanOneCycleGroup = errors.New("component has more than one cycle group")
)

// SCC - represents a strongly connected component in the graph. It may contain one vertex (table) with no cycles
// or multiple vertexes (vertexes) with cycles.
type SCC struct {
	// id - the unique identifier of the component
	id int
	// cyclesGraph - graph of cycles in the component and their links.
	//
	// This graph can be used to generate correct SQL queries to check the integrity of the group of cycles.
	cyclesGraph cyclesgraph.Graph
	// sccGraph - contains the mapping of the vertexes in the component to the edges in the original graph
	// if the component contains one vertex and no edges, then there is only one vertex with no cycles.
	sccGraph map[int][]tablegraph.Edge
	// vertexes - the vertexes in the component.
	vertexes map[int]common.Table
}

// NewSCC - creates a new SCC instance.
//
// It receives the unique identifier of the component, the graph of the component (it contains a table graph edges)
// and the vertexes that are part of the component.
//
// sccGraph - is a graph of table id to the edges that are connected to the table.
// vertexes - the vertexes that are part of the component. You can use an index from the sccGraph to get the table
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

// SubsetConditions - returns the subset conditions for the component.
//
// It includes the subset conditions for all the vertexes in the component.
func (c *SCC) SubsetConditions() []string {
	var subsetConditions []string
	for _, table := range c.vertexes {
		if len(table.SubsetConditions) > 0 {
			subsetConditions = append(subsetConditions, table.SubsetConditions...)
		}
	}
	return subsetConditions
}

// HasCycle - returns true if the component has cycles.
func (c *SCC) HasCycle() bool {
	return c.cyclesGraph.HasCycle()
}

// CyclesGroupCount - returns the count of the cycles group in the component.
func (c *SCC) CyclesGroupCount() int {
	return c.cyclesGraph.CyclesGroupCount()
}

// Vertexes - returns the vertexes in the component.
func (c *SCC) Vertexes() []common.Table {
	var vertexes []common.Table
	for _, table := range c.vertexes {
		vertexes = append(vertexes, table)
	}
	return vertexes
}
