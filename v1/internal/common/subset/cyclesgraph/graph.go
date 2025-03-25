package cyclesgraph

import (
	"fmt"
	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
	"slices"
	"sort"
	"strings"
)

// Graph - contains the cycles in the component and the graph of the cycles.
//
// It requires to generate the correct recursive SQL queries to check the integrity of the group of cycles.
// It also uses for joining two groups of cycles and checking the integrity between them.
type Graph struct {
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
	// graph - contains the mapping of the vertexes in the component to the edges in the original graph
	// for grouped cycles. This required to join the separated cycles together.
	//
	// For example, we found two cycles 1->2->3 and 2->3->4. The group will be 1,2,3 and 2,3,4. But they have to be
	// joined for integrity checks, because one group may produce records that are not satisfying the conditions
	// in the other group.
	graph map[string][]Edge
}

func NewGraph(
	componentGraph map[int][]tablegraph.Edge,
) Graph {
	g := Graph{
		cyclesIdents:  make(map[string]struct{}),
		groupedCycles: make(map[string][]int),
		graph:         make(map[string][]Edge),
	}
	g.findCycles(componentGraph)
	g.groupCycles()
	g.buildCyclesGraph()

	return g
}

func (g *Graph) HasCycle() bool {
	return len(g.cycles) > 0
}

func (g *Graph) CyclesGroupCount() int {
	return len(g.groupedCycles)
}

// findCycles - finds all cycles in the component
func (g *Graph) findCycles(componentGraph map[int][]tablegraph.Edge) {
	visited := make(map[int]bool)
	var path []tablegraph.Edge
	recStack := make(map[int]bool)

	// Collect and sort all vertexes
	var vertexes []int
	for v := range componentGraph {
		vertexes = append(vertexes, v)
	}
	sort.Ints(vertexes) // Ensure deterministic order

	for _, v := range vertexes {
		if !visited[v] {
			g.findAllCyclesDfs(componentGraph, v, visited, recStack, path)
		}
	}
}

// findAllCyclesDfs - the basic DFS algorithm adapted to find all cycles in the graph of components and
// collect the cycle vertices.
func (g *Graph) findAllCyclesDfs(
	componentGraph map[int][]tablegraph.Edge,
	v int,
	visited map[int]bool,
	recStack map[int]bool,
	path []tablegraph.Edge,
) {
	visited[v] = true
	recStack[v] = true

	// Sort edges to ensure deterministic order
	var edges []tablegraph.Edge
	edges = append(edges, componentGraph[v]...)
	sort.Slice(edges, func(i, j int) bool {
		return edges[i].To().TableID() < edges[j].To().TableID()
	})

	for _, to := range edges {

		path = append(path, to)
		if !visited[to.To().TableID()] {
			g.findAllCyclesDfs(componentGraph, to.To().TableID(), visited, recStack, path)
		} else if recStack[to.To().TableID()] {
			// Cycle detected
			var cycle []tablegraph.Edge
			for idx := len(path) - 1; idx >= 0; idx-- {
				cycle = append(cycle, path[idx])
				if path[idx].From().TableID() == to.To().TableID() {
					break
				}
			}
			cycleId := getCycleId(cycle)
			if _, ok := g.cyclesIdents[cycleId]; !ok {
				res := slices.Clone(cycle)
				slices.Reverse(res)
				g.cycles = append(g.cycles, res)
				g.cyclesIdents[cycleId] = struct{}{}
			}
		}
		path = path[:len(path)-1]
	}

	recStack[v] = false
}

// groupCycles - groups the cycles by the vertexes. It builds the map where the key is the group id and the value
// is the list of the cycles indexes.
func (g *Graph) groupCycles() {
	for cycleIdx, cycle := range g.cycles {
		cycleId := getCycleGroupId(cycle)
		g.groupedCycles[cycleId] = append(g.groupedCycles[cycleId], cycleIdx)
	}
}

// buildCyclesGraph - builds the graph of the grouped cycles. It contains the mapping of the vertexes in the component
func (g *Graph) buildCyclesGraph() {
	var idSeq int
	// Cast the map keys to the slice to have deterministic order for each run.
	cyclesGroups := make([]string, 0, len(g.groupedCycles))
	for group := range g.groupedCycles {
		cyclesGroups = append(cyclesGroups, group)
	}
	sort.Strings(cyclesGroups)
	for _, groupIdI := range cyclesGroups {
		cyclesI := g.groupedCycles[groupIdI]
		for _, groupIdJ := range cyclesGroups {
			cyclesJ := g.groupedCycles[groupIdJ]
			if groupIdI == groupIdJ {
				continue
			}
			commonVertexes := g.findCommonVertexes(cyclesI[0], cyclesJ[0])
			if len(commonVertexes) == 0 {
				continue
			}
			if g.areCyclesLinked(cyclesI[0], cyclesJ[0]) {
				continue
			}
			e := NewEdge(idSeq, groupIdI, groupIdJ, commonVertexes)
			g.graph[groupIdI] = append(g.graph[groupIdJ], e)
			idSeq++
		}
	}
}

// findCommonVertexes - finds the common vertexes between the cycles.
func (g *Graph) findCommonVertexes(i, j int) []common.Table {
	commonTables := make(map[string]common.Table)
	for _, edgeI := range g.cycles[i] {
		for _, edgeJ := range g.cycles[j] {
			if edgeI.To().TableID() == edgeJ.To().TableID() {
				tableName := edgeI.To().FullTableName()
				commonTables[tableName] = edgeI.To().Table()
			}
		}
	}
	var res []common.Table
	for _, table := range commonTables {
		res = append(res, table)
	}
	slices.SortFunc(res, func(i, j common.Table) int {
		return strings.Compare(i.TableName(), j.TableName())
	})
	return res
}

// areCyclesLinked - checks if the cycles are linked by the vertexes.
//
// It checks if the cycles have the same vertexes in the edges of cycles. If they have the common vertexes
// then they are linked.
//
// For example, we have two cycles 1->2->3 and 2->3->4. The group will be 1,2,3 and 2,3,4. The cycles are linked
// because they have the common vertex 2,3. Those 2 and 3 vertexes can be used to join the cycles.
func (g *Graph) areCyclesLinked(i, j int) bool {
	iId := getCycleGroupId(g.cycles[i])
	jId := getCycleGroupId(g.cycles[j])
	for _, to := range g.graph[iId] {
		if to.to == jId {
			return true
		}
	}
	for _, to := range g.graph[jId] {
		if to.to == iId {
			return true
		}
	}
	return false
}

// getCycleGroupId - returns the group id for the cycle based on the vertexes ID.
//
// For example, we have two cycles 1->2->3 and 2->3->4. The group will be 1,2,3 and 2,3,4.
// The group id will be 1_2_3 and 2_3_4.
func getCycleGroupId(cycle []tablegraph.Edge) string {
	ids := make([]string, 0, len(cycle))
	for _, edge := range cycle {
		ids = append(ids, fmt.Sprintf("%d", edge.To().TableID()))
	}
	slices.Sort(ids)
	return strings.Join(ids, "_")
}

// getCycleId - returns the unique identifier for the cycle based on the edges ID.
//
// For example, we have two similar cycles 1->2->3 (with edges 11->12->13) and 1->2->3 (with edges 21->22->23).
// Then the unique identifier will be 11_12_13 and 21_22_23.
func getCycleId(cycle []tablegraph.Edge) string {
	ids := make([]string, 0, len(cycle))
	for _, edge := range cycle {
		ids = append(ids, fmt.Sprintf("%d", edge.ID()))
	}
	slices.Sort(ids)
	return strings.Join(ids, "_")
}
