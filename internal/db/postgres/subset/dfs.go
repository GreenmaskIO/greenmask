package subset

import "slices"

const (
	vertexIsNotVisited = iota
	vertexIsVisitedAndPrecessing
	vertexIsVisitedAndCompleted
)

const (
	emptyFromValue = -1
)

// getCycle returns the cycle in the graph provided based on the from slice gathered in findCycleDfs function
func getCycle(from []int, lastEdge int) []int {
	var cycle []int
	for e := from[lastEdge]; e != lastEdge; e = from[e] {
		cycle = append(cycle, e)
	}
	cycle = append(cycle, lastEdge)
	slices.Reverse(cycle)
	return cycle
}

// FindAllCycles returns all cycles in the graph provided
// The result contains a slice of cycles, where each cycle is a slice of vertices in the order they appear in the cycle
func FindAllCycles(graph [][]*Edge) [][]int {
	var allCycles [][]int
	visited := make([]int, len(graph))
	from := make([]int, len(graph))
	for idx := range from {
		from[idx] = emptyFromValue
	}
	for v := range graph {
		if visited[v] == vertexIsNotVisited {
			findAllCyclesDfs(graph, v, visited, from, &allCycles)
		}
	}
	return allCycles
}

// findAllCyclesDfs - the basic DFS algorithm adapted to find all cycles in the graph and collect the cycle vertices
func findAllCyclesDfs(graph [][]*Edge, v int, visited []int, from []int, allCycles *[][]int) {
	visited[v] = vertexIsVisitedAndPrecessing
	for _, to := range graph[v] {
		if visited[to.Idx] == vertexIsNotVisited {
			from[to.Idx] = v
			findAllCyclesDfs(graph, to.Idx, visited, from, allCycles)
		} else if visited[to.Idx] == vertexIsVisitedAndPrecessing {
			from[to.Idx] = v
			cycle := getCycle(from, to.Idx)
			*allCycles = append(*allCycles, cycle)
		}
	}
	visited[v] = vertexIsVisitedAndCompleted
}
