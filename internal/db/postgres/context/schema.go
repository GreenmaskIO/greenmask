package context

import (
	"context"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func getDatabaseSchema(
	ctx context.Context, tx pgx.Tx, options *pgdump.Options, version int,
) ([]*toolkit.Table, error) {
	var tables []*toolkit.Table
	query, err := buildSchemaIntrospectionQuery(
		options.Table, options.ExcludeTable,
		options.IncludeForeignData, options.Schema,
		options.ExcludeSchema,
	)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		table := &toolkit.Table{}
		err = rows.Scan(
			&table.Oid, &table.Schema, &table.Name, &table.Kind,
			&table.Parent, &table.Children,
		)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	// fill columns
	for _, table := range tables {
		// We do not exclude generated columns here, because the schema must be compared with the original
		columns, err := getColumnsConfig(ctx, tx, table.Oid, version, false)
		if err != nil {
			return nil, err
		}
		table.Columns = columns
	}

	// 1. Find partitioned tables
	// 2. Find all children of partitioned tables
	// 3. Find children in the tables
	// 4. Set RootPtSchema, RootPtName, RootPtOid for children
	for _, table := range tables {
		if table.Kind != "p" || table.Parent != 0 {
			continue
		}
		for _, ptOId := range table.Children {
			idx := slices.IndexFunc(tables, func(table *toolkit.Table) bool {
				return table.Oid == ptOId
			})
			if idx == -1 {
				log.Debug().
					Int("TableOid", int(ptOId)).
					Msg("table might be excluded: unable to find partitioned table")
				continue
			}
			t := tables[idx]
			t.RootPtName = table.Name
			t.RootPtSchema = table.Schema
			t.RootPtOid = table.Oid
		}
	}
	return tables, nil
}
