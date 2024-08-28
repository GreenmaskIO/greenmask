package context

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func getDatabaseSchema(
	ctx context.Context, tx pgx.Tx, options *pgdump.Options, version int,
) ([]*toolkit.Table, error) {
	var res []*toolkit.Table
	query, err := BuildSchemaIntrospectionQuery(
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
		res = append(res, table)
	}

	// fill columns
	for _, table := range res {
		// We do not exclude generated columns here, because the schema must be compared with the original
		columns, err := getColumnsConfig(ctx, tx, table.Oid, version, false)
		if err != nil {
			return nil, err
		}
		table.Columns = columns
	}

	return res, nil
}
