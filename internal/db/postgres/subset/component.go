package subset

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
)

type Component struct {
	id int
	// componentGraph - contains the mapping of the vertexes in the component to the edges in the original graph
	// if the component contains one vertex and no edges, then there is only one vertex with no cycles
	componentGraph map[int][]*Edge
	// tables - the vertexes in the component
	tables map[int]*entries.Table
	// Cycles
	cycles       [][]*Edge
	cyclesIdents map[string]struct{}
	keys         []string
}

func NewComponent(id int, componentGraph map[int][]*Edge, tables map[int]*entries.Table) *Component {
	c := &Component{
		id:             id,
		componentGraph: componentGraph,
		tables:         tables,
		cyclesIdents:   make(map[string]struct{}),
	}
	c.findCycles()
	if c.hasCycle() {
		c.keys = c.getComponentKeys()
	} else {
		c.keys = c.getOneTable().PrimaryKey
	}

	return c
}

func (c *Component) getSubsetConds() []string {
	var subsetConds []string
	for _, table := range c.tables {
		if len(table.SubsetConds) > 0 {
			subsetConds = append(subsetConds, table.SubsetConds...)
		}
	}
	return subsetConds
}

func (c *Component) getOneTable() *entries.Table {
	if !c.hasCycle() {
		for _, table := range c.tables {
			return table
		}
	}
	panic("cannot call get one table method for cycled scc")
}

func (c *Component) hasCycle() bool {
	return len(c.cycles) > 0
}

// findCycles - finds all cycles in the component
func (c *Component) findCycles() {
	visited := make(map[int]bool)
	var path []*Edge
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
func (c *Component) findAllCyclesDfs(v int, visited map[int]bool, recStack map[int]bool, path []*Edge) {
	visited[v] = true
	recStack[v] = true

	// Sort edges to ensure deterministic order
	var edges []*Edge
	edges = append(edges, c.componentGraph[v]...)
	sort.Slice(edges, func(i, j int) bool {
		return edges[i].to.idx < edges[j].to.idx
	})

	for _, to := range edges {

		path = append(path, to)
		if !visited[to.idx] {
			c.findAllCyclesDfs(to.idx, visited, recStack, path)
		} else if recStack[to.idx] {
			// Cycle detected
			var cycle []*Edge
			for idx := len(path) - 1; idx >= 0; idx-- {
				cycle = append(cycle, path[idx])
				if path[idx].from.idx == to.to.idx {
					break
				}
			}
			cycleId := getCycleIdent(cycle)
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

func getCycleIdent(cycle []*Edge) string {
	ids := make([]string, 0, len(cycle))
	for _, edge := range cycle {
		ids = append(ids, fmt.Sprintf("%d", edge.id))
	}
	slices.Sort(ids)
	return strings.Join(ids, "_")
}

func (c *Component) getComponentKeys() []string {
	if len(c.cycles) > 1 {
		panic("IMPLEMENT ME: multiple cycles in the component")
	}
	if !c.hasCycle() {
		return c.getOneTable().PrimaryKey
	}

	var vertexes []int
	for _, edge := range c.cycles[0] {
		vertexes = append(vertexes, edge.to.idx)
	}

	var keys []string
	for _, v := range vertexes {
		table := c.tables[v]
		for _, key := range table.PrimaryKey {
			keys = append(keys, fmt.Sprintf(`%s__%s__%s`, table.Schema, table.Name, key))
		}
	}
	return keys
}
