package context

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/config"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/dump"
	"github.com/wwoytenko/greenfuscator/internal/domains"
	toolkit "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
)

// ValidateAndBuildTableConfig - validates tables, transformers and their parameters. Builds config for tables and returns
// ValidationWarnings that can be used for checking helpers in configuring and debugging transformation. Those
// may contain the schema affection warnings that would be useful for considering consistency
func validateAndBuildTablesConfig(
	ctx context.Context, tx pgx.Tx, cfg []*config.Table, tm map[string]*toolkit.Definition,
) (map[toolkit.Oid]*dump.Table, toolkit.ValidationWarnings, error) {
	tables := make(map[toolkit.Oid]*dump.Table, len(cfg))
	var warnings toolkit.ValidationWarnings

	for _, t := range cfg {
		table, tableWarnings, err := getTable(ctx, tx, t.Schema, t.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot build table from config: %w", err)
		}
		warnings = append(warnings, tableWarnings...)
		if len(tableWarnings) > 0 {
			continue
		}

		// Assign table constraints
		constraints, err := getTableConstraints(ctx, tx, table.Oid)
		if err != nil {
			return nil, nil, err
		}
		table.Constraints = constraints

		// Assign columns and transformersMap if were found
		columns, err := getColumnsConfig(ctx, tx, table.Oid)
		if err != nil {
			return nil, nil, err
		}
		table.Columns = columns

		// InitTransformation transformers
		if len(t.Transformers) > 0 {
			for _, tc := range t.Transformers {
				transformer, initWarnings, err := initTransformer(ctx, table, tc, tx.Conn().TypeMap(), tm)
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

func getTable(ctx context.Context, tx pgx.Tx, schema, name string) (*dump.Table, toolkit.ValidationWarnings, error) {
	table := &dump.Table{}
	var warnings toolkit.ValidationWarnings

	row := tx.QueryRow(ctx, TableSearchQuery, schema, name)
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
	} else if err != nil {
		return nil, nil, fmt.Errorf("cannot scan table: %w", err)
	}
	return table, warnings, nil
}

func getColumnsConfig(ctx context.Context, tx pgx.Tx, oid toolkit.Oid) ([]*toolkit.Column, error) {
	var res []*toolkit.Column
	rows, err := tx.Query(ctx, TableColumnsQuery, oid)
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

func getTableConstraints(ctx context.Context, tx pgx.Tx, oid toolkit.Oid) ([]toolkit.Constraint, error) {
	var constraints []toolkit.Constraint

	rows, err := tx.Query(ctx, TableConstraintsCommonQuery, oid)
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
			&constraintOid, &name, &schema, &constraintType,
			&rootPtConstraint, &fkTable, &constrainedColumns,
			&referencesColumns, &definition,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to build constraints list: %w", err)
		}

	}

	return constraints, nil
}
