package subset

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var (
	edgeSequence = 0
)

var (
	foreignKeyColumnsQuery = `
		SELECT n.nspname            as             fk_table_schema,
			   fk_ref_table.relname as             fk_table_name,
			   array_agg(curr_table_attrs.attname) curr_table_columns
		FROM pg_catalog.pg_constraint curr_table_con
				 join pg_catalog.pg_class fk_ref_table on curr_table_con.confrelid = fk_ref_table.oid
				 join pg_catalog.pg_namespace n on fk_ref_table.relnamespace = n.oid
				 join pg_catalog.pg_attribute curr_table_attrs on curr_table_attrs.attrelid = curr_table_con.conrelid AND
																  curr_table_attrs.attnum = ANY (curr_table_con.conkey)
		WHERE curr_table_con.conrelid = $1
		  AND curr_table_con.contype = 'f'
		GROUP BY fk_table_schema, fk_table_name, curr_table_con.oid;
	`
)

type TableLink struct {
	Idx   int
	Table *entries.Table
	Keys  []string
}

func NewTableLink(idx int, t *entries.Table, keys []string) *TableLink {
	return &TableLink{
		Idx:   idx,
		Table: t,
		Keys:  keys,
	}
}

type Edge struct {
	Id  int
	Idx int
	A   *TableLink
	B   *TableLink
}

func NewEdge(idx int, a *TableLink, b *TableLink) *Edge {
	edgeSequence++
	return &Edge{
		Id:  edgeSequence,
		Idx: idx,
		A:   a,
		B:   b,
	}
}

func (e *Edge) GetLeftAndRightTable(idx int) (*TableLink, *TableLink) {
	if e.A.Idx == idx {
		return e.A, e.B
	}
	return e.B, e.A
}

func GenerateSubsetQueriesForTable(ctx context.Context, tx pgx.Tx, tables []*entries.Table) error {
	// TODO: Add cycle validation

	orientedGraph := make([][]*Edge, len(tables))
	nonOrientedGraph := make([][]*Edge, len(tables))

	for idx, table := range tables {
		refs, err := getReferences(ctx, tx, table.Oid)
		if err != nil {
			return fmt.Errorf("error getting references: %w", err)
		}
		for _, ref := range refs {
			foreignTableIdx := slices.IndexFunc(tables, func(t *entries.Table) bool {
				return t.Name == ref.Name && t.Schema == ref.Schema
			})

			if foreignTableIdx == -1 {
				log.Debug().
					Str("Schema", ref.Schema).
					Str("Table", ref.Name).
					Msg("unable to find foreign table: it might be excluded from the dump")
				continue
			}

			orientedGraph[idx] = append(
				orientedGraph[idx],
				NewEdge(
					foreignTableIdx,
					NewTableLink(idx, table, ref.ReferencedKeys),
					NewTableLink(foreignTableIdx, tables[foreignTableIdx], tables[foreignTableIdx].PrimaryKey),
				),
			)

			nonOrientedGraph[idx] = append(
				nonOrientedGraph[idx],
				NewEdge(
					foreignTableIdx,
					NewTableLink(idx, table, ref.ReferencedKeys),
					NewTableLink(foreignTableIdx, tables[foreignTableIdx], tables[foreignTableIdx].PrimaryKey),
				),
			)
			nonOrientedGraph[foreignTableIdx] = append(
				nonOrientedGraph[foreignTableIdx],
				NewEdge(
					idx,
					NewTableLink(foreignTableIdx, tables[foreignTableIdx], tables[foreignTableIdx].PrimaryKey),
					NewTableLink(idx, table, ref.ReferencedKeys),
				),
			)

		}
	}

	subsetedVertexes := findSubsetVertexes(orientedGraph, tables)
	for v, path := range subsetedVertexes {
		setQueriesV2(path, v, orientedGraph, tables)
	}
	return nil
}

func getReferences(ctx context.Context, tx pgx.Tx, tableOid toolkit.Oid) ([]*toolkit.Reference, error) {
	var refs []*toolkit.Reference
	rows, err := tx.Query(ctx, foreignKeyColumnsQuery, tableOid)
	if err != nil {
		return nil, fmt.Errorf("error executing ForeignKeyColumnsQuery: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		ref := &toolkit.Reference{}
		if err = rows.Scan(&ref.Schema, &ref.Name, &ref.ReferencedKeys); err != nil {
			return nil, fmt.Errorf("error scanning ForeignKeyColumnsQuery: %w", err)
		}
		refs = append(refs, ref)
	}
	return refs, nil
}
