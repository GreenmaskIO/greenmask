package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"golang.org/x/exp/slices"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/config"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/data_section"
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

func BuildTablesConfig(ctx context.Context, tx pgx.Tx, tableConfig []*config.Table) (map[data_section.Oid]*data_section.Table, domains.ValidationWarnings, error) {
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

	tables := make(map[data_section.Oid]*data_section.Table, len(tableConfig))
	var warnings domains.ValidationWarnings

	for _, t := range tableConfig {
		table := &data_section.Table{
			TypeMap: tx.Conn().TypeMap(),
		}

		row := tx.QueryRow(ctx, tableSearchQuery, t.Schema, t.Name)
		err := row.Scan(&table.Oid, &table.Schema, &table.Name, &table.Owner, &table.RelKind,
			&table.RootPtSchema, &table.RootPtName, &table.Root,
		)

		if err != nil && errors.Is(err, pgx.ErrNoRows) {
			warnings = append(warnings, domains.NewValidationWarning().
				SetMsgf("table %s.%s not found", table.Schema, table.Name).
				SetLevel(domains.ErrorValidationSeverity).
				AddMeta("Level", TableValidationLevel).
				AddMeta("SchemaName", table.Schema).
				AddMeta("TableName", table.Name),
			)
			continue
		} else if err != nil {
			return nil, nil, fmt.Errorf("cannot scan tableSearchQuery: %w", err)
		}

		// Assign table constraints
		constraints, err := getTableConstraints(ctx, tx, table)
		if err != nil {
			return nil, nil, err
		}
		table.Constraints = constraints

		// Assign columns and transformersMap if were found
		columns, err := getColumnsConfig(ctx, tx, table)
		if err != nil {
			return nil, nil, err
		}
		table.Columns = columns

		// InitTransformation transformers
		if len(t.TransformersConfig) > 0 {
			for _, tc := range t.TransformersConfig {
				transformer, err := initTransformer(table, tc, tx.Conn().TypeMap())
				var re *domains.RuntimeError
				if err != nil && errors.As(err, &re) {
					// TODO: You should rewrite it because here you are translation RuntimeError to ValidationWarning
					w := domains.NewValidationWarning().SetMsg(re.Msg).SetLevel(domains.ErrorValidationSeverity)
					w.Meta = re.Meta
					if re.Err != nil {
						w.AddMeta("Err", re.Err.Error())
					}
					warnings = append(warnings, w)
				} else if err != nil {
					return nil, nil, err
				}
				table.Transformers = append(table.Transformers, transformer)
			}
		}

		tables[table.Oid] = table
	}

	return tables, warnings, nil
}

func getColumnsConfig(ctx context.Context, tx pgx.Tx, table *data_section.Table) ([]*data_section.Column, error) {

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

	var res []*data_section.Column
	rows, err := tx.Query(ctx, tableColumnsQuery, table.Oid)
	if err != nil {
		return nil, fmt.Errorf("unable execute tableColumnQuery: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var column data_section.Column
		if err = rows.Scan(&column.Name, &column.TypeOid, &column.TypeName,
			&column.NotNull, &column.Length, &column.Num); err != nil {
			return nil, fmt.Errorf("cannot scan tableColumnQuery: %w", err)
		}
		res = append(res, &column)
	}

	return res, nil
}

func initTransformer(table *data_section.Table, transformerConf *config.TransformerConfig, typeMap *pgtype.Map) (domains.Transformer, error) {
	transformerMaker, ok := transformers.TransformerMap[transformerConf.Name]
	if !ok {
		return nil, domains.NewRuntimeError().
			SetErr(ErrColumnNotFound).
			SetLevel(zerolog.ErrorLevel).
			AddMeta("Level", TransformerValidationLevel).
			AddMeta("SchemaName", table.Schema).
			AddMeta("TableName", table.Name).
			AddMeta("TransformerName", transformerConf.Name).
			SetErr(ErrTransformerNotFound)
	}
	if transformerMaker.Settings.TransformationType == domains.AttributeTransformation {
		columnName, ok := transformerConf.Params["column"]
		if !ok {
			return nil, domains.NewRuntimeError().
				SetErr(ErrColumnNotFound).
				SetLevel(zerolog.ErrorLevel).
				AddMeta("Level", TransformerValidationLevel).
				AddMeta("SchemaName", table.Schema).
				AddMeta("TableName", table.Name).
				AddMeta("TransformerName", transformerConf.Name).
				SetErr(fmt.Errorf(`parameter "column" is required for attribute transformers`))
		}
		if !slices.ContainsFunc(table.Columns, func(column *data_section.Column) bool {
			return column.Name == columnName
		}) {
			return nil, domains.NewRuntimeError().
				SetErr(ErrColumnNotFound).
				SetLevel(zerolog.ErrorLevel).
				AddMeta("Level", TransformerValidationLevel).
				AddMeta("SchemaName", table.Schema).
				AddMeta("TableName", table.Name).
				AddMeta("TransformerName", transformerConf.Name).
				SetErr(fmt.Errorf(`column %s is not found`, columnName))
		}
	}

	transformer, err := transformerMaker.InstanceTransformer(table, typeMap, transformerConf.Params)
	if err != nil {
		return nil, domains.NewRuntimeError().
			SetErr(ErrColumnNotFound).
			SetLevel(zerolog.ErrorLevel).
			AddMeta("Level", TransformerValidationLevel).
			AddMeta("SchemaName", table.Schema).
			AddMeta("TableName", table.Name).
			AddMeta("TransformerName", transformerConf.Name).
			SetErr(fmt.Errorf("transformer initialization error: %w", err))
	}
	return transformer, nil
}

func getTableConstraints(ctx context.Context, tx pgx.Tx, table *data_section.Table) ([]*data_section.Constraint, error) {
	var constraints []*data_section.Constraint

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
		return nil, fmt.Errorf("cannot execute tableConstraintsQuery: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var c data_section.Constraint
		err = rows.Scan(
			&c.Name, &c.Schema, &c.ConstraintType,
			&c.Domain, &c.RootPtConstraint, &c.FkTable,
			&c.ConstrainedColumns,
			&c.ReferencesColumns,
			&c.ReferencedTables,
			&c.Definition,
		)
		if err != nil {
			return nil, fmt.Errorf("cannot scan tableConstraintsQuery: %w", err)
		}
		constraints = append(constraints, &c)
	}

	return constraints, nil
}

func GetObjects(ctx context.Context, tx pgx.Tx, pgDumpOptions *pgdump.Options, tablesConfig map[data_section.Oid]*data_section.Table, dumpIdSeq *data_section.DumpId) ([]*data_section.Table, []*data_section.Sequence, error) {

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
	sequences := make([]*data_section.Sequence, 0)
	tables := make([]*data_section.Table, 0)
	defer rows.Close()
	for rows.Next() {
		var oid data_section.Oid
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
		var table *data_section.Table

		switch relKind {
		case 'S':
			sequences = append(sequences, &data_section.Sequence{
				Name:        name,
				Schema:      schemaName,
				Oid:         data_section.Oid(oid),
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
				table = &data_section.Table{
					Name:                 name,
					Schema:               schemaName,
					Oid:                  oid,
					Owner:                owner,
					DumpId:               dumpIdSeq.GetDumpId(),
					RelKind:              relKind,
					RootPtSchema:         rootPtSchema,
					RootPtName:           rootPtName,
					ExcludeData:          excludeData,
					LoadViaPartitionRoot: pgDumpOptions.LoadViaPartitionRoot,
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

func setTableColumns(ctx context.Context, tx pgx.Tx, table *data_section.Table) error {
	tableColumnsQuery := `
		SELECT 
		    a.attname,
		    a.atttypid 	as typeoid,
		  	pg_catalog.format_type(a.atttypid, a.atttypmod) as typename
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	cfg := make(map[string]*data_section.Column, 0)
	for _, c := range table.Columns {
		cfg[c.Name] = c
	}

	rows, err := tx.Query(ctx, tableColumnsQuery, table.Oid)
	if err != nil {
		return fmt.Errorf("perform query: %w", err)
	}
	columns := make([]*data_section.Column, 0)
	for rows.Next() {
		column := data_section.Column{}
		if err = rows.Scan(&column.Name, &column.TypeOid, &column.TypeName); err != nil {
			return fmt.Errorf("cannot scan column: %w", err)
		}

		columns = append(columns, &column)
	}

	table.Columns = columns
	return nil
}
