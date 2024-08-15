package subset

func SetSubsetQueries(graph *Graph) error {
	graph.buildCondensedGraph()
	graph.findSubsetVertexes()
	for _, p := range graph.paths {
		if isPathForScc(p, graph) {
			graph.generateAndSetQueryForScc(p)
		} else {
			graph.generateAndSetQueryForTable(p)
		}
	}
	return nil
}
