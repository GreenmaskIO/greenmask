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

// FindCycle returns the cycle in the graph provided
// The result contains the cycle vertices in the order they appear in the cycle
func FindCycle(graph [][]*Edge) []int {
	visited := make([]int, len(graph))
	from := make([]int, len(graph))
	for idx := range from {
		from[idx] = emptyFromValue
	}
	for v := range graph {
		// Contains is required for same reason as in TopologicalSort function
		if visited[v] == 0 {
			cycle := findCycleDfs(graph, v, visited, from)
			if len(cycle) > 0 {
				return cycle
			}
		}
	}
	return nil
}

// getCycle returns the cycle in the graph provided based on the from slice gathered in findCycleDfs function
func getCycle(from []int, lastVertex int) []int {
	var cycle []int
	for v := from[lastVertex]; v != lastVertex; v = from[v] {
		cycle = append(cycle, v)
	}
	cycle = append(cycle, lastVertex)
	slices.Reverse(cycle)
	return cycle
}

// findCycleDfs - the basic DFS algorithm adapted to find the cycle in the graph and collect the cycle vertices
func findCycleDfs(graph [][]*Edge, v int, visited []int, from []int) []int {
	visited[v] = vertexIsVisitedAndPrecessing
	for _, to := range graph[v] {
		if visited[to.Idx] == vertexIsNotVisited {
			from[to.Idx] = v
			cycle := findCycleDfs(graph, to.Idx, visited, from)
			// Return upper in stack the cycle
			if len(cycle) > 0 {
				return cycle
			}
		} else if visited[to.Idx] == vertexIsVisitedAndPrecessing {
			// Graph has cycle
			// If the cycle is found, then restore it
			// Find the vertex where the cycle starts
			from[to.Idx] = v
			return getCycle(from, to.Idx)
		}
	}
	visited[v] = vertexIsVisitedAndCompleted
	return nil
}
