package config_builder

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/dump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/toclib"
	"github.com/wwoytenko/greenfuscator/internal/domains"
	toolkit "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
)

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

func SetTableColumns(ctx context.Context, tx pgx.Tx, table *toclib.Table) error {

	cfg := make(map[string]*toclib.Column)
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
