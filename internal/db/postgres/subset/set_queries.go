package subset

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
)

func SetSubsetQueries(ctx context.Context, tx pgx.Tx, tables []*entries.Table) error {
	graph, err := NewGraph(ctx, tx, tables)
	if err != nil {
		return fmt.Errorf("error creating graph: %w", err)
	}
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
