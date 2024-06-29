package subset

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/jackc/pgx/v5"
)

func SetSubsetQueries(ctx context.Context, tx pgx.Tx, tables []*entries.Table) error {
	graph, err := NewGraph(ctx, tx, tables)
	if err != nil {
		return fmt.Errorf("error creating graph: %w", err)
	}
	graph.findSubsetVertexes()
	for _, p := range graph.Paths {
		generateAndSetQuery(p, tables)
	}
	return nil
}
