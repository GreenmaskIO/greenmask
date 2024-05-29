package subset

import "slices"

// TopologicalSort returns the topological sort of the graph provided
func TopologicalSort(graph [][]*Edge, path []int) []int {
	visited := make([]int, len(graph))
	order := make([]int, 0, len(graph))
	var component int
	for _, v := range path {
		//
		if visited[v] == 0 {
			component++
			order = topologicalSortDfs(graph, v, visited, order, component, path)
		}
	}
	//slices.Reverse(order)
	return order
}

// topologicalSortDfs - the basic DFS algorithm adapted to find the topological sort
func topologicalSortDfs(graph [][]*Edge, v int, visited []int, order []int, component int, path []int) []int {
	visited[v] = component
	for _, to := range graph[v] {
		if visited[to.Idx] == 0 && slices.Contains(path, to.Idx) {
			order = topologicalSortDfs(graph, to.Idx, visited, order, component, path)
		}
	}
	return append(order, v)
}
