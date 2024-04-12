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

// findGraphComponents returns slice of components where the first element is the number of components
// and the second element is the slice of components idxs
func findGraphComponents(graph [][]int) [][]int {
	visited := make([]int, len(graph))
	var component int
	for v := range graph {
		if visited[v] == vertexIsNotVisited {
			component++
			componentDfs(graph, v, visited, component)
		}
	}

	totalComponents := component
	components := make([][]int, totalComponents)
	component = 1
	for idx := range visited {
		components[visited[idx]-1] = append(components[visited[idx]-1], idx)
	}
	return components
}

// componentDfs - the basic DFS algorithm adapted to find the strongly connected components
func componentDfs(graph [][]int, v int, visited []int, component int) {
	visited[v] = component
	for _, to := range graph[v] {
		if visited[to] == vertexIsNotVisited {
			componentDfs(graph, to, visited, component)
		}
	}
}

// topologicalSort returns the topological sort of the graph provided
// In greenmask we use topological sort after:
// 1. Finding the strongly connected components
// 2. Excluding the cycles from the graph
func topologicalSort(graph [][]int) []int {
	visited := make([]int, len(graph))
	order := make([]int, 0, len(graph))
	var component int
	for v := range graph {
		if visited[v] == 0 {
			component++
			order = topologicalSortDfs(graph, v, visited, order, component)
		}
	}
	slices.Reverse(order)
	return order
}

// topologicalSortDfs - the basic DFS algorithm adapted to find the topological sort
func topologicalSortDfs(graph [][]int, v int, visited []int, order []int, component int) []int {
	visited[v] = component
	for _, to := range graph[v] {
		if visited[to] == 0 {
			order = topologicalSortDfs(graph, to, visited, order, component)
		}
	}
	return append(order, v)
}

// findCycle returns the cycle in the graph provided
// The result contains the cycle vertices in the order they appear in the cycle
func findCycle(graph [][]int) []int {
	visited := make([]int, len(graph))
	from := make([]int, len(graph))
	for idx := range from {
		from[idx] = emptyFromValue
	}
	for v := range graph {
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
func findCycleDfs(graph [][]int, v int, visited []int, from []int) []int {
	visited[v] = vertexIsVisitedAndPrecessing
	for _, to := range graph[v] {
		if visited[to] == vertexIsNotVisited {
			from[to] = v
			cycle := findCycleDfs(graph, to, visited, from)
			// Return upper in stack the cycle
			if len(cycle) > 0 {
				return cycle
			}
		} else if visited[to] == vertexIsVisitedAndPrecessing {
			// Graph has cycle
			// If the cycle is found, then restore it
			// Find the vertex where the cycle starts
			from[to] = v
			return getCycle(from, to)
		}
	}
	visited[v] = vertexIsVisitedAndCompleted
	return nil
}
