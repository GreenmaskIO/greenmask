package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pg_catalog"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgdump"
	"github.com/wwoytenko/greenfuscator/internal/transformers"
)

func setTableColumnsTransformers(ctx context.Context, tx pgx.Tx, table *domains.Table) error {
	tableColumnsQuery := `
		SELECT 
		    a.attname,
		    a.atttypid 	as typeoid,
		  	pg_catalog.format_type(a.atttypid, a.atttypmod) as typename,
		  	a.attnotnull
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	cfg := make(map[string]domains.Column, 0)
	for _, c := range table.Columns {
		cfg[c.Name] = c
	}

	rows, err := tx.Query(ctx, tableColumnsQuery, table.Oid)
	if err != nil {
		return fmt.Errorf("perform query: %w", err)
	}
	columns := make([]domains.Column, 0)
	for rows.Next() {
		column := domains.Column{}
		if err = rows.Scan(&column.Name, &column.TypeOid, &column.Type, &column.NotNull); err != nil {
			return fmt.Errorf("cannot scan column: %w", err)
		}

		if c, ok := cfg[column.Name]; ok {
			transformerConf := c.TransformConf
			makeTransformer, ok := transformers.TransformerMap[transformerConf.Name]
			if !ok {
				return fmt.Errorf("unable to init transformer \"%s\" for table %s.%s on column %s: unnable to find transformer with name %s", transformerConf.Name, table.Schema, table.Name, column.Name, transformerConf.Name)
			}
			column.TransformConf = transformerConf
			transformer, err := makeTransformer.NewTransformer(column.ColumnMeta, tx.Conn().TypeMap(), "", c.TransformConf.Params)
			if err != nil {
				return fmt.Errorf("unable to init transformer \"%s\" for table %s.%s on column %s: %w", transformerConf.Name, table.Schema, table.Name, column.Name, err)
			}
			column.Transformer = transformer
			table.HasTransformer = true
		}

		columns = append(columns, column)
	}

	table.Columns = columns
	return nil
}

func buildObjects(ctx context.Context, tx pgx.Tx, pgDumpOptions *pgdump.Options, tableConfig []domains.Table, dumpIdSeq *domains.DumpIdSequence) ([]*domains.Table, []*domains.Sequence, error) {

	cfg := make(map[string]domains.Table, len(tableConfig))
	for _, item := range tableConfig {
		cfg[fmt.Sprintf("%s.%s", item.Schema, item.Name)] = item
	}

	// Building relation search query using regexp adaptation rules and pre-defined query templates
	query, err := pg_catalog.BuildTableSearchQuery(pgDumpOptions.Table, pgDumpOptions.ExcludeTable,
		pgDumpOptions.ExcludeTableData, pgDumpOptions.IncludeForeignData, pgDumpOptions.Schema,
		pgDumpOptions.ExcludeSchema)
	if err != nil {
		return nil, nil, err
	}

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("perform query: %w", err)
	}

	// Generate table objects
	sequences := make([]*domains.Sequence, 0)
	tables := make([]*domains.Table, 0)
	defer rows.Close()
	for rows.Next() {
		var oid int
		var lastVal int64
		var schemaName, name, owner, rootPtName, rootPtSchema string
		var relKind rune
		var excludeData, isCalled bool

		if err = rows.Scan(&oid, &schemaName, &name, &owner, &relKind,
			&rootPtSchema, &rootPtName, &excludeData, &isCalled, &lastVal,
		); err != nil {
			return nil, nil, fmt.Errorf("unnable scan data: %w", err)
		}
		switch relKind {
		case 'S':
			sequences = append(sequences, &domains.Sequence{
				Name:        name,
				Schema:      schemaName,
				Oid:         oid,
				Owner:       owner,
				DumpId:      dumpIdSeq.GetDumpId(),
				LastValue:   lastVal,
				IsCalled:    isCalled,
				ExcludeData: excludeData,
			})
		case 'r':
			fallthrough
		case 'p':
			fallthrough
		case 'f':
			var columns []domains.Column
			t, ok := cfg[fmt.Sprintf("%s.%s", schemaName, name)]
			if ok {
				columns = t.Columns
			}
			table := &domains.Table{
				Oid:                  oid,
				Name:                 name,
				Schema:               schemaName,
				Columns:              columns,
				Query:                t.Query,
				Owner:                owner,
				DumpId:               dumpIdSeq.GetDumpId(),
				RelKind:              relKind,
				RootPtSchema:         rootPtSchema,
				RootPtName:           rootPtName,
				ExcludeData:          excludeData,
				LoadViaPartitionRoot: pgDumpOptions.LoadViaPartitionRoot,
			}

			tables = append(tables, table)
		default:
			return nil, nil, fmt.Errorf("unknown relkind \"%s\"", relKind)
		}
	}

	// Assign columns and transformers for table
	for _, table := range tables {
		if err := setTableColumnsTransformers(ctx, tx, table); err != nil {
			return nil, nil, err
		}
	}

	return tables, sequences, nil
}
