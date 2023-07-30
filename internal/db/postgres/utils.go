package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"

	pgdomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pg_catalog"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgdump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/transformers"
	domains "github.com/wwoytenko/greenfuscator/internal/domains"
)

var (
	ErrTransformerNotFound = errors.New("transformer not found")
	ErrColumnNotFound      = errors.New("column not found")
)

const (
	TransformerValidationLevel = "Transformer"
	InternalValidationLevel    = "Internal"
	ColumnValidationLevel      = "Column"
	TableValidationLevel       = "Table"
)

const (
	FatalErrorLevel   = "fatal"
	WarningErrorLevel = "warning"
)

type ValidationErrors []error

func (ves ValidationErrors) IsFatal() bool {
	return slices.ContainsFunc(ves, func(err error) bool {
		switch v := err.(type) {
		case *domains.RuntimeError:
			return v.Level == zerolog.ErrorLevel
		default:
			return true

		}
	})
}

func (ves ValidationErrors) LogErrors() {
	for _, err := range ves {
		switch v := err.(type) {
		case *domains.RuntimeError:
			v.Log()
		default:
			log.Warn().Err(err).Msgf("internal error")
		}
	}
}

func BuildTablesConfig(ctx context.Context, tx pgx.Tx, tableConfig []*pgdomains.Table) (map[pgdomains.Oid]*pgdomains.Table, ValidationErrors) {
	tableSearchQuery := `
		SELECT 
		   c.oid::TEXT::INT, 
		   n.nspname                              as "Schema",
		   c.relname                              as "Name",
		   pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
		   c.relkind 							  as "RelKind",
		   (coalesce(pn.nspname, '')) 			  as "rootPtSchema",
		   (coalesce(pc.relname, '')) 			  as "rootPtName",
		   (coalesce(pc.oid, 0)) 			      as "rootOid"
        FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_catalog.pg_inherits i ON i.inhrelid = c.oid
                LEFT JOIN  pg_catalog.pg_class pc ON i.inhparent = pc.oid AND pc.relkind = 'p'
            	LEFT JOIN  pg_catalog.pg_namespace pn ON pc.relnamespace = pn.oid
        WHERE c.relkind IN ('r', 'f', 'p')
          AND n.nspname  = $1  -- schema inclusion
          AND c.relname = $2 -- relname inclusion
	`

	tables := make(map[pgdomains.Oid]*pgdomains.Table, len(tableConfig))
	var errs ValidationErrors

	for _, t := range tableConfig {
		table := t
		schemaName := table.Schema
		tableName := table.Name

		row := tx.QueryRow(ctx, tableSearchQuery, table.Schema, table.Name)
		err := row.Scan(&table.Oid, &table.Schema, &table.Name, &table.Owner, &table.RelKind,
			&table.RootPtSchema, &table.RootPtName, &table.Root,
		)

		if err != nil && errors.Is(err, pgx.ErrNoRows) {
			errs = append(errs, domains.NewRuntimeError().
				SetErr(fmt.Errorf("table %s.%s not found: %w", table.Schema, table.Name, err)).
				SetLevel(zerolog.ErrorLevel).
				AddMeta("Level", TableValidationLevel).
				AddMeta("SchemaName", schemaName).
				AddMeta("TableName", tableName),
			)
			continue
		} else if err != nil {
			errs = append(errs, domains.NewRuntimeError().
				SetErr(fmt.Errorf("cannot scan tableSearchQuery: %w", err)).
				SetLevel(zerolog.ErrorLevel).
				AddMeta("Level", TableValidationLevel).
				AddMeta("SchemaName", schemaName).
				AddMeta("TableName", tableName),
			)
			goto errHandle
		}

		// Transforming slice of column transformersMap to map
		transformersMap := make(map[string]*pgdomains.Column)

		for _, item := range table.Columns {
			if _, ok := transformersMap[item.Name]; ok {
				errs = append(errs, domains.NewRuntimeError().
					SetErr(fmt.Errorf("column doubled")).
					SetLevel(zerolog.ErrorLevel).
					AddMeta("Level", ColumnValidationLevel).
					AddMeta("SchemaName", schemaName).
					AddMeta("TableName", tableName).
					AddMeta("ColumnName", item.Name),
				)
				goto errHandle
			}
			transformersMap[item.Name] = item
		}
		t.TransformersMap = transformersMap

		// Assign table constraints
		constraints, constraintErrs := getTableConstraints(ctx, tx, t)
		if constraintErrs != nil {
			errs = append(errs, constraintErrs...)
			goto errHandle
		}
		t.Constraints = constraints

		// Assign columns and transformersMap if were found
		columns, columnErrs := getColumnsConfig(ctx, tx, t)
		if columnErrs != nil {
			errs = append(errs, columnErrs...)
		}
		table.TransformersMap = columns
		table.Columns = nil

		tables[table.Oid] = table
	}

errHandle:
	if errs != nil {
		return nil, errs
	}

	return tables, nil
}

func getColumnsConfig(ctx context.Context, tx pgx.Tx, table *pgdomains.Table) (map[string]*pgdomains.Column, ValidationErrors) {
	var errs ValidationErrors

	tableColumnsQuery := `
		SELECT 
		    a.attname,
		    a.atttypid 	as typeoid,
		  	pg_catalog.format_type(a.atttypid, a.atttypmod) as typename,
		  	a.attnotnull,
		  	a.atttypmod,
		  	a.attnum
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	res := make(map[string]*pgdomains.Column)
	rows, err := tx.Query(ctx, tableColumnsQuery, table.Oid)
	if err != nil {
		errs = append(errs, domains.NewRuntimeError().
			SetErr(fmt.Errorf("unable execute tableColumnQuery: %w", err)).
			SetLevel(zerolog.ErrorLevel).
			AddMeta("Level", ColumnValidationLevel).
			AddMeta("SchemaName", table.Schema).
			AddMeta("TableName", table.Name),
		)
		goto errHandle
	}
	defer rows.Close()

	for rows.Next() {
		var column pgdomains.Column
		if err = rows.Scan(&column.Name, &column.TypeOid, &column.TypeName,
			&column.NotNull, &column.Length, &column.Num); err != nil {
			errs = append(errs, domains.NewRuntimeError().
				SetErr(fmt.Errorf("cannot scan tableColumnQuery: %w", err)).
				SetLevel(zerolog.ErrorLevel).
				AddMeta("Level", ColumnValidationLevel).
				AddMeta("SchemaName", table.Schema).
				AddMeta("TableName", table.Name),
			)
			goto errHandle
		}
		// If column has transformer assign it
		if c, ok := table.TransformersMap[column.Name]; ok {
			column.TransformConf = c.TransformConf
			transformer, err := getTransformerConfig(table, column, tx.Conn().TypeMap())
			if err != nil {
				errs = append(errs, err)
			}
			column.Transformer = transformer
			res[column.Name] = &column
		}
	}

	for name, _ := range table.TransformersMap {
		if _, ok := res[name]; !ok {
			errs = append(errs, domains.NewRuntimeError().
				SetErr(ErrColumnNotFound).
				SetLevel(zerolog.ErrorLevel).
				AddMeta("Level", ColumnValidationLevel).
				AddMeta("SchemaName", table.Schema).
				AddMeta("TableName", table.Name).
				AddMeta("ColumnName", name),
			)
		}
	}

errHandle:

	if errs != nil {
		return nil, errs
	}

	return res, nil
}

func getTransformerConfig(table *pgdomains.Table, column pgdomains.Column, typeMap *pgtype.Map) (domains.Transformer, error) {
	makeTransformer, ok := transformers.TransformerMap[column.TransformConf.Name]
	if !ok {

		return nil, domains.NewRuntimeError().
			SetErr(ErrColumnNotFound).
			SetLevel(zerolog.ErrorLevel).
			AddMeta("Level", TransformerValidationLevel).
			AddMeta("SchemaName", table.Schema).
			AddMeta("TableName", table.Name).
			AddMeta("ColumnName", column.Name).
			AddMeta("TransformerName", column.TransformConf.Name).
			SetErr(ErrTransformerNotFound)
	}
	c, ok := table.TransformersMap[column.Name]
	if !ok {
		panic(fmt.Sprintf("column %s not found", column.Name))
	}
	// TODO: Refactor useType - it must be in transformer params instead
	transformer, err := makeTransformer.InstanceTransformer(&table.TableMeta, &column.ColumnMeta, typeMap, c.TransformConf.Params)
	if err != nil {
		return nil, domains.NewRuntimeError().
			SetErr(ErrColumnNotFound).
			SetLevel(zerolog.ErrorLevel).
			AddMeta("Level", TransformerValidationLevel).
			AddMeta("SchemaName", table.Schema).
			AddMeta("TableName", table.Name).
			AddMeta("ColumnName", column.Name).
			AddMeta("TransformerName", column.TransformConf.Name).
			SetErr(fmt.Errorf("transformer initialization error: %w", err))
	}
	return transformer, nil
}

func getTableConstraints(ctx context.Context, tx pgx.Tx, table *pgdomains.Table) ([]*pgdomains.Constraint, []error) {
	var errs []error
	var res []*pgdomains.Constraint

	tableConstraintsQuery := `
		SELECT pc.conname                                    AS "name",
			   pn.nspname                                    AS "schema",
			   pc.contype                                    AS "type",
			   pc.contypid::TEXT::INT                        AS domain_oid,
			   pc.conparentid::TEXT::INT                     AS root_pt_constraint_oid,
			   pc.confrelid::TEXT::INT                       AS fk_ref_oid,
			   pc.conkey                                     AS constrained_column_oids,
			   pc.confkey                                    AS constrained_column_fk_oids,
			   CASE
				   WHEN pc.contype = 'p' THEN
					   (SELECT array_agg(c.oid)
						FROM pg_catalog.pg_constraint c
						WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors(pc.conrelid)
											UNION ALL
											VALUES (pc.conrelid))
						  AND contype = 'f'
						  AND conparentid = 0)
				   END                                       AS referenced_tables,
			   pg_catalog.pg_get_constraintdef(pc.oid, true) AS def
		FROM pg_constraint pc
				 JOIN pg_namespace pn on pc.connamespace = pn.oid
		WHERE conrelid = $1
	`

	rows, err := tx.Query(ctx, tableConstraintsQuery, table.Oid)
	if err != nil {
		errs = append(errs, domains.NewRuntimeError().
			SetErr(fmt.Errorf("cannot execute tableConstraintsQuery: %w", err)).
			SetLevel(zerolog.ErrorLevel).
			AddMeta("Level", ColumnValidationLevel).
			AddMeta("SchemaName", table.Schema).
			AddMeta("TableName", table.Name),
		)

		goto errHandle
	}
	defer rows.Close()

	for rows.Next() {
		var c pgdomains.Constraint
		err = rows.Scan(
			&c.Name, &c.Schema, &c.Type,
			&c.Domain, &c.RootPtConstraint, &c.FkTable,
			&c.ConstrainedColumns,
			&c.ReferencesColumns,
			&c.ReferencedTable,
			&c.Def,
		)
		if err != nil {
			errs = append(errs, domains.NewRuntimeError().
				SetErr(fmt.Errorf("cannot scan tableConstraintsQuery: %w", err)).
				SetLevel(zerolog.ErrorLevel).
				AddMeta("Level", TableValidationLevel).
				AddMeta("SchemaName", table.Schema).
				AddMeta("TableName", table.Name),
			)
			goto errHandle
		}
		res = append(res, &c)
	}

errHandle:
	if errs != nil {
		return nil, errs
	}

	return res, nil
}

func GetObjects(ctx context.Context, tx pgx.Tx, pgDumpOptions *pgdump.Options, tablesConfig map[pgdomains.Oid]*pgdomains.Table, dumpIdSeq *pgdomains.DumpId) ([]*pgdomains.Table, []*pgdomains.Sequence, error) {

	// Building relation search query using regexp adaptation rules and pre-defined query templates
	// TODO: Refactor it to gotemplate
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
	sequences := make([]*pgdomains.Sequence, 0)
	tables := make([]*pgdomains.Table, 0)
	defer rows.Close()
	for rows.Next() {
		var oid pgdomains.Oid
		var lastVal int64
		var schemaName, name, owner, rootPtName, rootPtSchema string
		var relKind rune
		var excludeData, isCalled bool
		var ok bool

		if err = rows.Scan(&oid, &schemaName, &name, &owner, &relKind,
			&rootPtSchema, &rootPtName, &excludeData, &isCalled, &lastVal,
		); err != nil {
			return nil, nil, fmt.Errorf("unnable scan data: %w", err)
		}
		var table *pgdomains.Table

		switch relKind {
		case 'S':
			sequences = append(sequences, &pgdomains.Sequence{
				Name:        name,
				Schema:      schemaName,
				Oid:         pgdomains.Oid(oid),
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
			table, ok = tablesConfig[oid]
			if ok {
				// If table was discovered during Transformer validation - use that object instead of a new
				table.DumpId = dumpIdSeq.GetDumpId()
				table.ExcludeData = excludeData
				table.LoadViaPartitionRoot = pgDumpOptions.LoadViaPartitionRoot
			} else {
				// If not - create new table object
				table = &pgdomains.Table{
					Name:   name,
					Schema: schemaName,
					TableMeta: pgdomains.TableMeta{
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
			}

			tables = append(tables, table)
		default:
			return nil, nil, fmt.Errorf("unknown relkind \"%s\"", relKind)
		}
	}

	// Assign columns and transformers for table
	for _, table := range tables {
		if err := setTableColumns(ctx, tx, table); err != nil {
			return nil, nil, err
		}
	}

	return tables, sequences, nil
}

func setTableColumns(ctx context.Context, tx pgx.Tx, table *pgdomains.Table) error {
	tableColumnsQuery := `
		SELECT 
		    a.attname,
		    a.atttypid 	as typeoid,
		  	pg_catalog.format_type(a.atttypid, a.atttypmod) as typename
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	cfg := make(map[string]*pgdomains.Column, 0)
	for _, c := range table.Columns {
		cfg[c.Name] = c
	}

	rows, err := tx.Query(ctx, tableColumnsQuery, table.Oid)
	if err != nil {
		return fmt.Errorf("perform query: %w", err)
	}
	columns := make([]*pgdomains.Column, 0)
	for rows.Next() {
		column := pgdomains.Column{}
		if err = rows.Scan(&column.Name, &column.TypeOid, &column.TypeName); err != nil {
			return fmt.Errorf("cannot scan column: %w", err)
		}

		columns = append(columns, &column)
	}

	table.Columns = columns
	return nil
}
