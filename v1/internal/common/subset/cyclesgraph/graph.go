package cyclesgraph

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

// Graph - contains the Cycles in the component and the Graph of the Cycles.
//
// It requires to generate the correct recursive SQL queries to check the integrity of the group of Cycles.
// It also uses for joining two groups of Cycles and checking the integrity between them.
type Graph struct {
	// Graph - contains the mapping of the vertexes in the component to the edges in the original Graph
	// for grouped Cycles. This required to join the separated Cycles together.
	//
	// For example, we found two Cycles 1->2->3 and 2->3->4. The group will be 1,2,3 and 2,3,4. But they have to be
	// joined for integrity checks, because one group may produce records that are not satisfying the conditions
	// in the other group.
	Graph map[string][]Edge
	// Cycles - a list of Cycles in the component
	//
	// It contains list of edges that are involved in the cycle.
	Cycles [][]tablegraph.Edge

	// GroupedCycles - Cycles grouped by the vertexes.
	//
	// For example
	//	1. Two Cycles 1->2->3 and 1->2->3 - the both has the same vertexes. Then the group
	//     will be 1,2,3 for both.
	//  2. Two Cycles 1->2->3 and 2->3->4 - The group has different vertexes in the Cycles.
	//     The group will be 1,2,3 and 2,3,4.
	//
	// This later uses to determine the way how to check the integrity when we have multiple Cycles group.
	// Because if we have more than one group we need to check the integrity between them  joining them
	// and validate the conditions that the records appeared in one Cycles satisfies the conditions in the other.
	GroupedCycles map[string][]int
	// cyclesIdents - contains the unique identifiers for the Cycles. It uses to avoid duplicates
	// in the Cycles list.
	cyclesIdents map[string]struct{}
}

func NewGraph(
	componentGraph map[int][]tablegraph.Edge,
) Graph {
	g := Graph{
		cyclesIdents:  make(map[string]struct{}),
		GroupedCycles: make(map[string][]int),
		Graph:         make(map[string][]Edge),
	}
	g.findCycles(componentGraph)
	g.groupCycles()
	g.buildCyclesGraph()

	return g
}

func (g *Graph) HasCycle() bool {
	return len(g.Cycles) > 0
}

func (g *Graph) CyclesGroupCount() int {
	return len(g.GroupedCycles)
}

// findCycles - finds all Cycles in the component
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

// findAllCyclesDfs - the basic DFS algorithm adapted to find all Cycles in the Graph of components and
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
				g.Cycles = append(g.Cycles, res)
				g.cyclesIdents[cycleId] = struct{}{}
			}
		}
		path = path[:len(path)-1]
	}

	recStack[v] = false
}

// groupCycles - groups the Cycles by the vertexes. It builds the map where the key is the group id and the value
// is the list of the Cycles indexes.
func (g *Graph) groupCycles() {
	for cycleIdx, cycle := range g.Cycles {
		cycleId := getCycleGroupId(cycle)
		g.GroupedCycles[cycleId] = append(g.GroupedCycles[cycleId], cycleIdx)
	}
}

// buildCyclesGraph - builds the Graph of the grouped Cycles. It contains the mapping of the vertexes in the component
func (g *Graph) buildCyclesGraph() {
	var idSeq int
	// Cast the map keys to the slice to have deterministic order for each run.
	cyclesGroups := make([]string, 0, len(g.GroupedCycles))
	for group := range g.GroupedCycles {
		cyclesGroups = append(cyclesGroups, group)
	}
	sort.Strings(cyclesGroups)
	for _, groupIdI := range cyclesGroups {
		cyclesI := g.GroupedCycles[groupIdI]
		for _, groupIdJ := range cyclesGroups {
			cyclesJ := g.GroupedCycles[groupIdJ]
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
			g.Graph[groupIdI] = append(g.Graph[groupIdJ], e)
			idSeq++
		}
	}
}

// findCommonVertexes - finds the common vertexes between the Cycles.
func (g *Graph) findCommonVertexes(i, j int) []common.Table {
	commonTables := make(map[string]common.Table)
	for _, edgeI := range g.Cycles[i] {
		for _, edgeJ := range g.Cycles[j] {
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

// areCyclesLinked - checks if the Cycles are linked by the vertexes.
//
// It checks if the Cycles have the same vertexes in the edges of Cycles. If they have the common vertexes
// then they are linked.
//
// For example, we have two Cycles 1->2->3 and 2->3->4. The group will be 1,2,3 and 2,3,4. The Cycles are linked
// because they have the common vertex 2,3. Those 2 and 3 vertexes can be used to join the Cycles.
func (g *Graph) areCyclesLinked(i, j int) bool {
	iId := getCycleGroupId(g.Cycles[i])
	jId := getCycleGroupId(g.Cycles[j])
	for _, to := range g.Graph[iId] {
		if to.to == jId {
			return true
		}
	}
	for _, to := range g.Graph[jId] {
		if to.to == iId {
			return true
		}
	}
	return false
}

// getCycleGroupId - returns the group id for the cycle based on the vertexes ID.
//
// For example, we have two Cycles 1->2->3 and 2->3->4. The group will be 1,2,3 and 2,3,4.
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
// For example, we have two similar Cycles 1->2->3 (with edges 11->12->13) and 1->2->3 (with edges 21->22->23).
// Then the unique identifier will be 11_12_13 and 21_22_23.
func getCycleId(cycle []tablegraph.Edge) string {
	ids := make([]string, 0, len(cycle))
	for _, edge := range cycle {
		ids = append(ids, fmt.Sprintf("%d", edge.ID()))
	}
	slices.Sort(ids)
	return strings.Join(ids, "_")
}
