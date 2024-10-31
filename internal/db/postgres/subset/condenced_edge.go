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

func (e *CondensedEdge) hasPolymorphicExpressions() bool {
	return len(e.originalEdge.from.polymorphicExprs) > 0 || len(e.originalEdge.to.polymorphicExprs) > 0
}

// sortCondensedEdges - returns condensed graph vertices in topological order
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

// topologicalSortDfs - recursive function to visit all vertices of the graph
func topologicalSortDfs(graph [][]*CondensedEdge, v int, visited []bool, stack *[]int) {
	visited[v] = true
	for _, edge := range graph[v] {
		if !visited[edge.to.idx] {
			topologicalSortDfs(graph, edge.to.idx, visited, stack)
		}
	}
	*stack = append(*stack, v)
}
