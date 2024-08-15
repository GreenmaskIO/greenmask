package subset

type CondensedEdge struct {
	id           int
	from         *ComponentLink
	to           *ComponentLink
	originalEdge *Edge
}

func NewCondensedEdge(id int, from, to *ComponentLink, originalEdge *Edge) *CondensedEdge {
	return &CondensedEdge{
		id:           id,
		from:         from,
		to:           to,
		originalEdge: originalEdge,
	}
}

func sortCondensedEdges(graph [][]*CondensedEdge) []int {
	stack := make([]int, 0)
	visited := make([]bool, len(graph))
	for i := range graph {
		if !visited[i] {
			topologicalSortDfs(graph, i, visited, &stack)
		}
	}
	return stack
}

func topologicalSortDfs(graph [][]*CondensedEdge, v int, visited []bool, stack *[]int) {
	visited[v] = true
	for _, edge := range graph[v] {
		if !visited[edge.to.idx] {
			topologicalSortDfs(graph, edge.to.idx, visited, stack)
		}
	}
	*stack = append(*stack, v)
}
