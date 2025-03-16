package condencedgraph

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

var (
	errComponentHasMoreThanOneCycleGroup = errors.New("component has more than one cycle group")
)

// Component - represents a strongly connected component in the graph. It may contain one vertex (table) with no cycles
// or multiple vertexes (tables) with cycles.
type Component struct {
	// id - the unique identifier of the component
	id int
	// componentGraph - contains the mapping of the vertexes in the component to the edges in the original graph
	// if the component contains one vertex and no edges, then there is only one vertex with no cycles
	componentGraph map[int][]tablegraph.Edge
	// tables - the vertexes in the component
	tables map[int]common.Table
	// cycles - a list of cycles in the component
	//
	// It contains list of edges that are involved in the cycle.
	cycles [][]tablegraph.Edge
	// cyclesIdents - contains the unique identifiers for the cycles. It uses to avoid duplicates
	// in the cycles list.
	cyclesIdents map[string]struct{}
	// groupedCycles - cycles grouped by the vertexes.
	//
	// For example
	//	1. Two cycles 1->2->3 and 1->2->3 - the both has the same vertexes. Then the group
	//     will be 1,2,3 for both.
	//  2. Two cycles 1->2->3 and 2->3->4 - The group has different vertexes in the cycles.
	//     The group will be 1,2,3 and 2,3,4.
	//
	// This later uses to determine the way how to check the integrity when we have multiple cycles group.
	// Because if we have more than one group we need to check the integrity between them  joining them
	// and validate the conditions that the records appeared in one cycles satisfies the conditions in the other.
	groupedCycles map[string][]int
	// groupedCyclesGraph - contains the mapping of the vertexes in the component to the edges in the original graph
	// for grouped cycles. This required to join the separated cycles together
	groupedCyclesGraph map[string][]*CycleEdge
}

// NewComponent - creates a new Component instance.
func NewComponent(
	id int,
	componentGraph map[int][]tablegraph.Edge,
	tables map[int]common.Table,
) Component {
	c := Component{
		id:             id,
		componentGraph: componentGraph,
		tables:         tables,
		cyclesIdents:   make(map[string]struct{}),
	}
	c.findCycles()
	c.groupCycles()
	c.buildCyclesGraph()

	return c
}

// SubsetConditions - returns the subset conditions for the component.
//
// It includes the subset conditions for all the tables in the component.
func (c *Component) SubsetConditions() []string {
	var subsetConditions []string
	for _, table := range c.tables {
		if len(table.SubsetConditions) > 0 {
			subsetConditions = append(subsetConditions, table.SubsetConditions...)
		}
	}
	return subsetConditions
}

// HasCycle - returns true if the component has cycles.
func (c *Component) HasCycle() bool {
	return len(c.cycles) > 0
}

// Tables - returns the tables in the component.
func (c *Component) Tables() []common.Table {
	var tables []common.Table
	for _, table := range c.tables {
		tables = append(tables, table)
	}
	return tables
}

func (c *Component) GetOneCycleGroup() ([][]tablegraph.Edge, error) {
	if len(c.groupedCycles) == 1 {
		for _, g := range c.groupedCycles {
			var res [][]tablegraph.Edge
			for _, idx := range g {
				res = append(res, c.cycles[idx])
			}
			return res, nil
		}
	}
	return nil, errComponentHasMoreThanOneCycleGroup
}

// findCycles - finds all cycles in the component
func (c *Component) findCycles() {
	visited := make(map[int]bool)
	var path []tablegraph.Edge
	recStack := make(map[int]bool)

	// Collect and sort all vertices
	var vertices []int
	for v := range c.componentGraph {
		vertices = append(vertices, v)
	}
	sort.Ints(vertices) // Ensure deterministic order

	for _, v := range vertices {
		if !visited[v] {
			c.findAllCyclesDfs(v, visited, recStack, path)
		}
	}
}

// findAllCyclesDfs - the basic DFS algorithm adapted to find all cycles in the graph and collect the cycle vertices
func (c *Component) findAllCyclesDfs(v int, visited map[int]bool, recStack map[int]bool, path []tablegraph.Edge) {
	visited[v] = true
	recStack[v] = true

	// Sort edges to ensure deterministic order
	var edges []tablegraph.Edge
	edges = append(edges, c.componentGraph[v]...)
	sort.Slice(edges, func(i, j int) bool {
		return edges[i].To().Index() < edges[j].To().Index()
	})

	for _, to := range edges {

		path = append(path, to)
		if !visited[to.Index()] {
			c.findAllCyclesDfs(to.Index(), visited, recStack, path)
		} else if recStack[to.Index()] {
			// Cycle detected
			var cycle []tablegraph.Edge
			for idx := len(path) - 1; idx >= 0; idx-- {
				cycle = append(cycle, path[idx])
				if path[idx].From().Index() == to.To().Index() {
					break
				}
			}
			cycleId := getCycleId(cycle)
			if _, ok := c.cyclesIdents[cycleId]; !ok {
				res := slices.Clone(cycle)
				slices.Reverse(res)
				c.cycles = append(c.cycles, res)
				c.cyclesIdents[cycleId] = struct{}{}
			}
		}
		path = path[:len(path)-1]
	}

	recStack[v] = false
}

func (c *Component) groupCycles() {
	c.groupedCycles = make(map[string][]int)
	for cycleIdx, cycle := range c.cycles {
		cycleId := getCycleGroupId(cycle)
		c.groupedCycles[cycleId] = append(c.groupedCycles[cycleId], cycleIdx)
	}
}

func (c *Component) buildCyclesGraph() {
	// TODO: Need to loop through c.groupedCycles instead of c.cycles
	var idSeq int
	c.groupedCyclesGraph = make(map[string][]*CycleEdge)
	for groupIdI, cyclesI := range c.groupedCycles {
		for groupIdJ, cyclesJ := range c.groupedCycles {
			if groupIdI == groupIdJ {
				continue
			}
			commonVertexes := c.findCommonVertexes(cyclesI[0], cyclesJ[0])
			if len(commonVertexes) == 0 {
				continue
			}
			if c.areCyclesLinked(cyclesI[0], cyclesJ[0]) {
				continue
			}
			e := NewCycleEdge(idSeq, groupIdI, groupIdJ, commonVertexes)
			c.groupedCyclesGraph[groupIdI] = append(c.groupedCyclesGraph[groupIdJ], e)
			idSeq++
		}
	}
}

func (c *Component) findCommonVertexes(i, j int) (res []common.Table) {
	commonTables := make(map[string]common.Table)
	for _, edgeI := range c.cycles[i] {
		for _, edgeJ := range c.cycles[j] {
			if edgeI.To().Index() == edgeJ.To().Index() {
				tableName := edgeI.To().GetTableName()
				commonTables[tableName] = edgeI.To().Table()
			}
		}
	}
	for _, table := range commonTables {
		res = append(res, table)
	}
	slices.SortFunc(res, func(i, j common.Table) int {
		return strings.Compare(i.TableName(), j.TableName())
	})
	return
}

func (c *Component) areCyclesLinked(i, j int) bool {
	iId := getCycleGroupId(c.cycles[i])
	jId := getCycleGroupId(c.cycles[j])
	for _, to := range c.groupedCyclesGraph[iId] {
		if to.to == jId {
			return true
		}
	}
	for _, to := range c.groupedCyclesGraph[jId] {
		if to.to == iId {
			return true
		}
	}
	return false
}

func (c *Component) hasPolymorphicExpressions() bool {
	for _, edges := range c.componentGraph {
		for _, edge := range edges {
			if len(edge.From().PolymorphicExpressions()) > 0 {
				return true
			}
			if len(edge.To().PolymorphicExpressions()) > 0 {
				return true
			}
		}
	}
	return false
}

// getCycleGroupId - returns the group id for the cycle based on the vertexes ID.
func getCycleGroupId(cycle []tablegraph.Edge) string {
	ids := make([]string, 0, len(cycle))
	for _, edge := range cycle {
		ids = append(ids, fmt.Sprintf("%d", edge.To().Index()))
	}
	slices.Sort(ids)
	return strings.Join(ids, "_")
}

// getCycleId - returns the unique identifier for the cycle based on the edges ID.
func getCycleId(cycle []tablegraph.Edge) string {
	ids := make([]string, 0, len(cycle))
	for _, edge := range cycle {
		ids = append(ids, fmt.Sprintf("%d", edge.ID()))
	}
	slices.Sort(ids)
	return strings.Join(ids, "_")
}
