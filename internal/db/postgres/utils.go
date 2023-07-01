package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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
		case *ValidationError:
			return v.ErrorLevel == FatalErrorLevel
		default:
			return true

		}
	})
}

func (ves ValidationErrors) LogErrors() {
	for _, err := range ves {
		switch v := err.(type) {
		case *ValidationError:
			event := log.Warn().
				Str("ValidationLevel", v.Level).
				Str("ValidationErrorLevel", v.ErrorLevel)
			if v.Schema != "" {
				event.Str("SchemaName", v.Schema)
			}
			if v.Name != "" {
				event.Str("TableName", v.Name)
			}
			if v.Column != "" {
				event = event.Str("ColumnName", v.Column)
			}
			if v.Transformer != "" {
				event = event.Str("TransformerName", v.Transformer)
			}
			event.Err(v.Err).Msgf("validation error")

		default:
			log.Warn().Err(err).Msgf("internal error")
		}
	}
}

type ValidationError struct {
	Level       string `json:"type,omitempty"`
	ErrorLevel  string `json:"error-level,omitempty"`
	Schema      string `json:"schema,omitempty"`
	Name        string `json:"name,omitempty"`
	Column      string `json:"column,omitempty"`
	Transformer string `json:"transformer,omitempty"`
	Err         error  `json:"err,omitempty"`
}

func (ve ValidationError) Error() string {
	// TODO: Rewrite error formatting
	return fmt.Sprintf("%s validation error: %s: %s: %s: %s: %s", ve.Level, ve.Schema, ve.Name, ve.Column, ve.Transformer, ve.Err.Error())
}

func BuildTablesConfig(ctx context.Context, tx pgx.Tx, tableConfig []pgdomains.Table) (map[pgdomains.Oid]*pgdomains.Table, ValidationErrors) {
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
		schemaName := t.Schema
		tableName := t.Name

		row := tx.QueryRow(ctx, tableSearchQuery, t.Schema, t.Name)
		err := row.Scan(&t.Oid, &t.Schema, &t.Name, &t.Owner, &t.RelKind,
			&t.RootPtSchema, &t.RootPtName, &t.Root,
		)

		if err != nil && errors.Is(err, pgx.ErrNoRows) {
			errs = append(errs, &ValidationError{
				Level:      TableValidationLevel,
				ErrorLevel: FatalErrorLevel,
				Schema:     schemaName,
				Name:       tableName,
				Err:        fmt.Errorf("table %s.%s not found: %w", t.Schema, t.Name, err),
			})
			continue
		} else if err != nil {
			errs = append(errs, &ValidationError{
				Level:      TableValidationLevel,
				ErrorLevel: FatalErrorLevel,
				Schema:     schemaName,
				Name:       tableName,
				Err:        fmt.Errorf("cannot scan tableSearchQuery: %w", err),
			})
			goto errHandle
		}

		// Transforming slice of column transformersMap to map
		transformersMap := make(map[string]*pgdomains.Column)

		for _, item := range t.Columns {
			if _, ok := transformersMap[item.Name]; ok {
				errs = append(errs, &ValidationError{
					Level:      ColumnValidationLevel,
					ErrorLevel: FatalErrorLevel,
					Schema:     schemaName,
					Name:       tableName,
					Column:     item.Name,
					Err:        fmt.Errorf("column doubled"),
				})
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
		t.TransformersMap = columns

		tables[t.Oid] = &t
	}

errHandle:
	if errs != nil {
		return nil, errs
	}

	return tables, nil
}

func getColumnsConfig(ctx context.Context, tx pgx.Tx, table pgdomains.Table) (map[string]*pgdomains.Column, ValidationErrors) {
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
		errs = append(errs, &ValidationError{
			Level:      ColumnValidationLevel,
			ErrorLevel: FatalErrorLevel,
			Schema:     table.Schema,
			Name:       table.Name,
			Err:        fmt.Errorf("unable execute tableColumnQuery: %w", err),
		})
		goto errHandle
	}
	defer rows.Close()

	for rows.Next() {
		var column pgdomains.Column
		if err = rows.Scan(&column.Name, &column.TypeOid, &column.TypeName,
			&column.NotNull, &column.Length, &column.Num); err != nil {
			errs = append(errs, &ValidationError{
				Level:      ColumnValidationLevel,
				ErrorLevel: FatalErrorLevel,
				Schema:     table.Schema,
				Name:       table.Name,
				Err:        fmt.Errorf("cannot scan tableColumnQuery: %w", err),
			})
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
		}
		res[column.Name] = &column
	}

	for name, _ := range table.TransformersMap {
		if _, ok := res[name]; !ok {
			errs = append(errs, &ValidationError{
				Level:      ColumnValidationLevel,
				ErrorLevel: FatalErrorLevel,
				Schema:     table.Schema,
				Name:       table.Name,
				Column:     name,
				Err:        ErrColumnNotFound,
			})
		}
	}

errHandle:

	if errs != nil {
		return nil, errs
	}

	return res, nil
}

func getTransformerConfig(table pgdomains.Table, column pgdomains.Column, typeMap *pgtype.Map) (domains.Transformer, error) {
	makeTransformer, ok := transformers.TransformerMap[column.TransformConf.Name]
	if !ok {
		return nil, &ValidationError{
			Level:       TransformerValidationLevel,
			ErrorLevel:  FatalErrorLevel,
			Schema:      table.Schema,
			Name:        table.Name,
			Column:      column.Name,
			Transformer: column.TransformConf.Name,
			Err:         ErrTransformerNotFound,
		}
	}
	c, ok := table.TransformersMap[column.Name]
	if !ok {
		panic(fmt.Sprintf("column %s not found", column.Name))
	}
	// TODO: Refactor useType - it must be in transformer params instead
	transformer, err := makeTransformer.NewTransformer(&table.TableMeta, &column.ColumnMeta, typeMap, c.TransformConf.Params)
	if err != nil {
		return nil, &ValidationError{
			Level:       TransformerValidationLevel,
			ErrorLevel:  FatalErrorLevel,
			Schema:      table.Schema,
			Name:        table.Name,
			Column:      column.Name,
			Transformer: c.TransformConf.Name,
			Err:         err,
		}
	}
	return transformer, nil
}

func getTableConstraints(ctx context.Context, tx pgx.Tx, table pgdomains.Table) ([]*pgdomains.Constraint, []error) {
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
		errs = append(errs, &ValidationError{
			Level:      ColumnValidationLevel,
			ErrorLevel: FatalErrorLevel,
			Schema:     table.Schema,
			Name:       table.Name,
			Err:        fmt.Errorf("cannot execute tableConstraintsQuery: %w", err),
		})
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
			errs = append(errs, &ValidationError{
				Level:      TableValidationLevel,
				ErrorLevel: FatalErrorLevel,
				Schema:     table.Schema,
				Name:       table.Name,
				Err:        fmt.Errorf("cannot scan tableConstraintsQuery: %w", err),
			})
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

func setTableColumnsTransformers(ctx context.Context, tx pgx.Tx, table *pgdomains.Table) error {
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
			transformer, err := makeTransformer.NewTransformer(&table.TableMeta, &column.ColumnMeta, tx.Conn().TypeMap(), c.TransformConf.Params)
			if err != nil {
				return fmt.Errorf("unable to init transformer \"%s\" for table %s.%s on column %s: %w", transformerConf.Name, table.Schema, table.Name, column.Name, err)
			}
			column.Transformer = transformer
		}

		columns = append(columns, &column)
	}

	table.Columns = columns
	return nil
}

func buildObjects(ctx context.Context, tx pgx.Tx, pgDumpOptions *pgdump.Options, tableConfig []pgdomains.Table, dumpIdSeq *pgdomains.DumpId) ([]*pgdomains.Table, []*pgdomains.Sequence, error) {

	cfg := make(map[string]pgdomains.Table, len(tableConfig))
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
	sequences := make([]*pgdomains.Sequence, 0)
	tables := make([]*pgdomains.Table, 0)
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
			var columns []*pgdomains.Column
			t, ok := cfg[fmt.Sprintf("%s.%s", schemaName, name)]
			if ok {
				columns = t.Columns
			}
			table := &pgdomains.Table{
				Name:    name,
				Schema:  schemaName,
				Columns: columns,
				Query:   t.Query,
				TableMeta: pgdomains.TableMeta{
					Oid:                  pgdomains.Oid(oid),
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
