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

type ValidationError struct {
	ValidationType string
	Fatal          bool
	Schema         string
	Name           string
	Column         string
	err            error
}

func (ve *ValidationError) Error() string {
	return fmt.Sprintf("%s validation error: %s: %s: %s: %s", ve.ValidationType, ve.Schema, ve.Name, ve.Column, ve.err.Error())
}

func getTable() {

}

func getTableConstraints() {

}

func getTableTriggers() {

}

func BuildTablesConfig(ctx context.Context, tx pgx.Tx, tableConfig []domains.Table, dumpIdSeq *domains.DumpId) (map[domains.Oid]*domains.Table, []error) {
	tableSearchQuery := `
		SELECT 
		   c.oid::TEXT::INT, 
		   n.nspname                              as "Schema",
		   c.relname                              as "Name",
		   pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
		   c.relkind 							  as "RelKind",
		   (coalesce(pn.nspname, '')) 			  as "rootPtSchema",
		   (coalesce(pc.relname, '')) 			  as "rootPtName"
		   (coalesce(pc.oid, '')) 			      as "rootOid"
        FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_catalog.pg_inherits i ON i.inhrelid = c.oid
                LEFT JOIN  pg_catalog.pg_class pc ON i.inhparent = pc.oid AND pc.relkind = 'p'
            	LEFT JOIN  pg_catalog.pg_namespace pn ON pc.relnamespace = pn.oid
        WHERE c.relkind IN ('r', 'f', 'p')
          AND n.nspname  = $1  -- schema inclusion
          AND c.relname = $2 -- relname inclusion
	`

	tables := make(map[domains.Oid]*domains.Table, len(tableConfig))
	var errs []error

	for _, t := range tableConfig {
		var oid, rootOid domains.Oid
		var schemaName, name, owner, rootPtName, rootPtSchema string
		var relKind rune

		row := tx.QueryRow(ctx, tableSearchQuery, t.Name, t.Schema)
		err := row.Scan(&oid, &schemaName, &name, &owner, &relKind,
			&rootPtSchema, &rootPtName, &rootOid,
		)
		if err != nil {
			errs = append(errs, fmt.Errorf("unnable scan data: %w", err))
		}

		switch relKind {
		case 'r':
			fallthrough
		case 'p':
			fallthrough
		case 'f':
			// TODO: 1. getColumns
			//		 2. getConstraints

			table := &domains.Table{
				Name:    name,
				Schema:  schemaName,
				Columns: columns,
				Query:   t.Query,
				TableMeta: domains.TableMeta{
					Oid:          oid,
					Owner:        owner,
					DumpId:       dumpIdSeq.GetDumpId(),
					RelKind:      relKind,
					RootPtSchema: rootPtSchema,
					RootPtName:   rootPtName,
					Root:         rootOid,
					Constraints:,
				},
			}

			tables[table.Oid] = table
		default:
			errs = append(errs, fmt.Errorf(`BUG found: unknown relkind "%c"`, relKind))
		}
	}

	// Assign columns and transformers for table
	for _, table := range tables {
		if err := setTableColumnsTransformers(ctx, tx, table); err != nil {
			errs = append(errs, err)
		}
	}

	if errs != nil {
		return nil, errs
	}

	return tables, nil
}

func getColumnsConfig(ctx context.Context, tx pgx.Tx, tableOid domains.Oid, table *domains.Table) (map[string]domains.Column, []error) {
	var errs []error

	tableColumnsQuery := `
		SELECT 
		    a.attname,
		    a.atttypid 	as typeoid,
		  	pg_catalog.format_type(a.atttypid, a.atttypmod) as typename,
		  	a.attnotnull,
		  	a.atttypmod
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	res := make(map[string]domains.Column)
	rows, err := tx.Query(ctx, tableColumnsQuery, tableOid)
	for rows.Next() {
		var column domains.Column
		var name string
		if err = rows.Scan(&name, &column.TypeOid, &column.TypeName, &column.NotNull, &column.Length); err != nil {
			panic(fmt.Sprintf("cannot scan column: %s", err))
		}
		// If column has transformer assign it
		if c, ok := table.ColumnsMap[column.Name]; ok {
			transformerConf := c.TransformConf
			makeTransformer, ok := transformers.TransformerMap[transformerConf.Name]
			if !ok {
				errs = append(errs, &ValidationError{

				})
				return nil, fmt.Errorf("unable to init transformer \"%s\" for table %s.%s on column %s: unnable to find transformer with name %s")
			}
			column.TransformConf = transformerConf
			transformer, err := makeTransformer.NewTransformer(column.ColumnMeta, tx.Conn().TypeMap(), "", c.TransformConf.Params)
			if err != nil {
				return nil, fmt.Errorf("unable to init transformer \"%s\" for table %s.%s on column %s: %w", transformerConf.Name, table.Schema, table.Name, column.Name, err)
			}
			column.Transformer = transformer
		}
		res[name] = column
	}

	for k, v := range table.ColumnsMap {
	}

	return res, nil
}

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
		if err = rows.Scan(&column.Name, &column.TypeOid, &column.TypeName, &column.NotNull); err != nil {
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
		}

		columns = append(columns, column)
	}

	table.Columns = columns
	return nil
}

func buildObjects(ctx context.Context, tx pgx.Tx, pgDumpOptions *pgdump.Options, tableConfig []domains.Table, dumpIdSeq *domains.DumpId) ([]*domains.Table, []*domains.Sequence, error) {

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
				Name:    name,
				Schema:  schemaName,
				Columns: columns,
				Query:   t.Query,
				TableMeta: domains.TableMeta{
					Oid:                  oid,
					Owner:                owner,
					DumpId:               dumpIdSeq.GetDumpId(),
					RelKind:              relKind,
					RootPtSchema:         rootPtSchema,
					RootPtName:           rootPtName,
					ExcludeData:          excludeData,
					LoadViaPartitionRoot: pgDumpOptions.LoadViaPartitionRoot,
				},
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
