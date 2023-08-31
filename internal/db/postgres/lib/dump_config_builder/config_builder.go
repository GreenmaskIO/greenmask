package dump_config_builder

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/config"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/dump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/toclib"
	defaultTransformers "github.com/wwoytenko/greenfuscator/internal/db/postgres/transformers2"
	"github.com/wwoytenko/greenfuscator/internal/domains"
	toolkit "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
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

func BuildTablesConfig(ctx context.Context, tx pgx.Tx, tableConfig []*config.Table) (map[toolkit.Oid]*dump.Table, toolkit.ValidationWarnings, error) {
	transformersMap, err := buildTransformersMap()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot build transformer map: %w", err)
	}

	tables := make(map[toolkit.Oid]*dump.Table, len(tableConfig))
	var warnings toolkit.ValidationWarnings

	for _, t := range tableConfig {
		table := &dump.Table{}

		row := tx.QueryRow(ctx, TableSearchQuery, t.Schema, t.Name)
		err := row.Scan(&table.Oid, &table.Schema, &table.Name, &table.Owner, &table.RelKind,
			&table.RootPtSchema, &table.RootPtName, &table.RootOid,
		)

		if err != nil && errors.Is(err, pgx.ErrNoRows) {
			warnings = append(warnings, toolkit.NewValidationWarning().
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
				transformer, initWarnings, err := initTransformer(ctx, table, tc, tx.Conn().TypeMap(), transformersMap)
				if err != nil {
					return nil, nil, err
				}
				warnings = append(warnings, initWarnings...)
				table.Transformers = append(table.Transformers, transformer)
			}
		}

		tables[table.Oid] = table
	}

	return tables, warnings, nil
}

func getColumnsConfig(ctx context.Context, tx pgx.Tx, table *dump.Table) ([]*toolkit.Column, error) {
	var res []*toolkit.Column
	rows, err := tx.Query(ctx, TableColumnsQuery, table.Oid)
	if err != nil {
		return nil, fmt.Errorf("unable execute tableColumnQuery: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var column toolkit.Column
		if err = rows.Scan(&column.Name, &column.TypeOid, &column.TypeName,
			&column.NotNull, &column.Length, &column.Num); err != nil {
			return nil, fmt.Errorf("cannot scan tableColumnQuery: %w", err)
		}
		res = append(res, &column)
	}

	return res, nil
}

func buildTransformersMap() (map[string]*toolkit.Definition, error) {
	tm := make(map[string]*toolkit.Definition)
	for _, td := range defaultTransformers.DefaultTransformersList {
		if _, ok := tm[td.Properties.Name]; ok {
			return nil, fmt.Errorf("transformer with name %s already exists", td.Properties.Name)
		}
		tm[td.Properties.Name] = td
	}
	return tm, nil
}

func initTransformer(ctx context.Context, t *dump.Table, c *config.TransformerConfig, tm *pgtype.Map, dm map[string]*toolkit.Definition) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var totalWarnings toolkit.ValidationWarnings
	td, ok := dm[c.Name]
	if !ok {
		totalWarnings = append(totalWarnings,
			toolkit.NewValidationWarning().
				SetMsg("transformer not found").
				SetLevel(toolkit.ErrorValidationSeverity).SetTrace(&toolkit.Trace{
				SchemaName:      t.Schema,
				TableName:       t.Name,
				TransformerName: c.Name,
			}))
		return nil, totalWarnings, nil
	}
	driver, err := toolkit.NewDriver(tm, &t.Table)
	if err != nil {
		return nil, nil, fmt.Errorf("driver initialization for table %s.%s: %w", t.Schema, t.Name, err)
	}
	transformer, warnings, err := td.Instance(ctx, driver, c.Params)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to init transformer: %w", err)
	}
	return transformer, warnings, nil
}

func getTableConstraints(ctx context.Context, tx pgx.Tx, table *dump.Table) ([]toolkit.Constraint, error) {
	var constraints []toolkit.Constraint

	rows, err := tx.Query(ctx, TableConstraintsCommonQuery, table.Oid)
	if err != nil {
		return nil, fmt.Errorf("cannot execute tableConstraintsQuery: %w", err)
	}
	defer rows.Close()

	// Common constraints discovering
	for rows.Next() {
		var c toolkit.Constraint
		var oid toolkit.Oid
		var name, schema, definition string
		var constraintType rune
		var rootPtConstraint toolkit.Oid
		var fkTable toolkit.Oid
		var constrainedColumns, referencesColumns []toolkit.AttNum

		err = rows.Scan(
			&oid, &name, &schema, &constraintType,
			&rootPtConstraint, &fkTable, &constrainedColumns,
			&referencesColumns, &definition,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to build constraints list: %w", err)
		}

		switch constraintType {
		case 'f':
			c = &toolkit.ForeignKey{
				DefaultConstraintDefinition: toolkit.DefaultConstraintDefinition{
					ConstraintSchema:   schema,
					ConstraintName:     name,
					ConstraintOid:      oid,
					ConstrainedColumns: constrainedColumns,
					Definition:         definition,
				},
			}
		case 'c':
			c = &toolkit.Check{
				ConstraintSchema:   schema,
				ConstraintName:     name,
				ConstraintOid:      oid,
				ConstrainedColumns: constrainedColumns,
				Definition:         definition,
			}
		case 'p':
			c = &toolkit.PrimaryKey{
				ConstraintSchema:   schema,
				ConstraintName:     name,
				ConstraintOid:      oid,
				ConstrainedColumns: constrainedColumns,
				Definition:         definition,
			}
		case 'u':
			c = &toolkit.Unique{
				ConstraintSchema:   schema,
				ConstraintName:     name,
				ConstraintOid:      oid,
				ConstrainedColumns: constrainedColumns,
				Definition:         definition,
			}
		case 't':
			c = &toolkit.TriggerConstraint{
				ConstraintSchema:   schema,
				ConstraintName:     name,
				ConstraintOid:      oid,
				ConstrainedColumns: constrainedColumns,
				Definition:         definition,
			}
		case 'x':
			c = &toolkit.Exclusion{
				ConstraintSchema:   schema,
				ConstraintName:     name,
				ConstraintOid:      oid,
				ConstrainedColumns: constrainedColumns,
				Definition:         definition,
			}
		default:
			return nil, fmt.Errorf("unknown constraint type %c", constraintType)
		}

		if err != nil {
			return nil, fmt.Errorf("cannot scan tableConstraintsQuery: %w", err)
		}
		constraints = append(constraints, c)
	}

	// Foreign key constraint discovering
	rows, err = tx.Query(ctx, TablePrimaryKeyReferencesConstraintsQuery, table.Oid)
	if err != nil {
		return nil, fmt.Errorf("cannot execute tableConstraintsQuery: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var c toolkit.Constraint
		var constraintOid, onTableOid toolkit.Oid
		var name, schema, definition, onTableSchema, onTableName string
		var constraintType rune
		var rootPtConstraint toolkit.Oid
		var fkTable toolkit.Oid
		var constrainedColumns, referencesColumns []toolkit.AttNum

		err = rows.Scan(
			&oid, &name, &schema, &constraintType,
			&rootPtConstraint, &fkTable, &constrainedColumns,
			&referencesColumns, &definition,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to build constraints list: %w", err)
		}

	}

	return constraints, nil
}

func setTableColumns(ctx context.Context, tx pgx.Tx, table *toclib.Table) error {

	cfg := make(map[string]*toclib.Column, 0)
	for _, c := range table.Columns {
		cfg[c.Name] = c
	}

	rows, err := tx.Query(ctx, TableColumnsQuery, table.Oid)
	if err != nil {
		return fmt.Errorf("perform query: %w", err)
	}
	columns := make([]*toclib.Column, 0)
	for rows.Next() {
		column := toclib.Column{}
		if err = rows.Scan(&column.Name, &column.TypeOid, &column.TypeName); err != nil {
			return fmt.Errorf("cannot scan column: %w", err)
		}

		columns = append(columns, &column)
	}

	table.Columns = columns
	return nil
}
