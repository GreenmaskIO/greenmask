package subset

import "slices"

type Table struct {
	Name       string
	Schema     string
	PrimaryKey []string
	Reference  []*Reference
}

type Reference struct {
	Name       string
	Schema     string
	ForeignKey []string
}

func buildSimpleGraph(tables []*Table) [][]int {
	graph := make([][]int, len(tables))
	for idx, table := range tables {
		for _, ref := range table.Reference {
			foreignTableIdx := slices.IndexFunc(tables, func(t *Table) bool {
				return t.Name == ref.Name && t.Schema == ref.Schema
			})
			if foreignTableIdx != -1 {
				graph[idx] = append(graph[idx], foreignTableIdx)
			} else {
				panic("foreign table not found")
			}
		}
	}
	return graph
}
