package context

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
	transformersUtils "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

// ValidateAndBuildTableConfig - validates tables, toolkit and their parameters. Builds config for tables and returns
// ValidationWarnings that can be used for checking helpers in configuring and debugging transformation. Those
// may contain the schema affection warnings that would be useful for considering consistency
func validateAndBuildTablesConfig(
	ctx context.Context, tx pgx.Tx, typeMap *pgtype.Map,
	cfg []*domains.Table, registry *transformersUtils.TransformerRegistry,
	version int, types []*toolkit.Type,
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
		constraints, err := getTableConstraints(ctx, tx, table.Oid, version)
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

		driver, err := toolkit.NewDriver(typeMap, table.Table, t.ColumnsTypeOverride)
		if err != nil {
			return nil, nil, fmt.Errorf("unnable to initialise driver: %w", err)
		}
		table.Driver = driver

		// InitTransformation toolkit
		if len(t.Transformers) > 0 {
			for _, tc := range t.Transformers {
				transformer, initWarnings, err := initTransformer(ctx, driver, tc, typeMap, registry, types)
				if len(initWarnings) > 0 {
					for _, w := range initWarnings {
						// Enriching the table context into meta
						w.AddMeta("SchemaName", table.Schema).
							AddMeta("TableName", table.Name).
							AddMeta("TransformerName", tc.Name)

					}
				}
				if err != nil {
					return nil, warnings, err
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
	table := &dump.Table{
		Table: &toolkit.Table{},
	}
	var warnings toolkit.ValidationWarnings

	row := tx.QueryRow(ctx, TableSearchQuery, schema, name)
	err := row.Scan(&table.Oid, &table.Schema, &table.Name, &table.Owner, &table.RelKind,
		&table.RootPtSchema, &table.RootPtName, &table.RootOid,
	)

	if err != nil && errors.Is(err, pgx.ErrNoRows) {
		warnings = append(warnings, toolkit.NewValidationWarning().
			SetMsgf("table %s.%s not found", table.Schema, table.Name).
			SetSeverity(toolkit.ErrorValidationSeverity).
			//AddMeta("Severity", TableValidationLevel).
			AddMeta("Schema", table.Schema).
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

func getTableConstraints(ctx context.Context, tx pgx.Tx, tableOid toolkit.Oid, version int) (
	[]toolkit.Constraint, error,
) {
	var constraints []toolkit.Constraint

	rows, err := tx.Query(ctx, TableConstraintsCommonQuery, tableOid)
	if err != nil {
		return nil, fmt.Errorf("cannot execute TableConstraintsCommonQuery: %w", err)
	}
	defer rows.Close()

	// Common constraints discovering
	var pk *toolkit.PrimaryKey
	for rows.Next() {
		var c toolkit.Constraint
		var constraintOid toolkit.Oid
		var constraintName, constraintSchema, constraintDefinition, rtName, rtSchema string
		var constraintType rune
		var rtOid toolkit.Oid // rt - referenced table
		var constraintColumns, rtColumns []toolkit.AttNum

		err = rows.Scan(
			&constraintOid, &constraintName, &constraintSchema, &constraintType, &constraintColumns,
			&rtOid, &rtName, &rtSchema, &rtColumns, &constraintDefinition,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to build constraints list: %w", err)
		}

		switch constraintType {
		case 'f':
			// TODO: Recheck it
			c = &toolkit.ForeignKey{
				DefaultConstraintDefinition: toolkit.DefaultConstraintDefinition{
					Schema:     constraintSchema,
					Name:       constraintName,
					Oid:        constraintOid,
					Columns:    constraintColumns,
					Definition: constraintDefinition,
				},
			}
		case 'c':
			c = &toolkit.Check{
				Schema:     constraintSchema,
				Name:       constraintName,
				Oid:        constraintOid,
				Columns:    constraintColumns,
				Definition: constraintDefinition,
			}
		case 'p':
			pk = toolkit.NewPrimaryKey(constraintSchema, constraintName, constraintDefinition, constraintOid, constraintColumns)
			c = pk
		case 'u':
			c = &toolkit.Unique{
				Schema:     constraintSchema,
				Name:       constraintName,
				Oid:        constraintOid,
				Columns:    constraintColumns,
				Definition: constraintDefinition,
			}
		case 't':
			c = &toolkit.TriggerConstraint{
				Schema:     constraintSchema,
				Name:       constraintName,
				Oid:        constraintOid,
				Columns:    constraintColumns,
				Definition: constraintDefinition,
			}
		case 'x':
			c = &toolkit.Exclusion{
				Schema:     constraintSchema,
				Name:       constraintName,
				Oid:        constraintOid,
				Columns:    constraintColumns,
				Definition: constraintDefinition,
			}
		default:
			return nil, fmt.Errorf("unknown constraint type %c", constraintType)
		}

		if err != nil {
			return nil, fmt.Errorf("cannot scan tableConstraintsQuery: %w", err)
		}
		constraints = append(constraints, c)
	}

	// Add FK references to PK
	buf := bytes.NewBuffer(nil)
	err = TablePrimaryKeyReferencesConstraintsQuery.Execute(
		buf,
		struct {
			Version int
		}{
			Version: version,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error templating TablePrimaryKeyReferencesConstraintsQuery: %w", err)
	}
	log.Debug().Str("query", buf.String()).Msg("TablePrimaryKeyReferencesConstraintsQuery templating result")
	fkRows, err := tx.Query(ctx, buf.String(), tableOid)
	if err != nil {
		return nil, fmt.Errorf("cannot execute tableConstraintsQuery: %w", err)
	}
	defer fkRows.Close()

	for fkRows.Next() {
		var constraintOid, onTableOid toolkit.Oid
		var constraintName, constraintSchema, constraintDefinition, onTableSchema, onTableName string
		var constraintColumns []toolkit.AttNum

		err = fkRows.Scan(
			&constraintOid, &constraintSchema, &constraintName, &onTableOid,
			&onTableSchema, &onTableName, &constraintColumns, &constraintDefinition,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to build constraints list: %w", err)
		}

		pk.References = append(pk.References, &toolkit.LinkedTable{
			Oid:    onTableOid,
			Schema: onTableSchema,
			Name:   onTableName,
			Constraint: &toolkit.ForeignKey{
				DefaultConstraintDefinition: toolkit.DefaultConstraintDefinition{
					Schema:     constraintSchema,
					Name:       constraintName,
					Oid:        constraintOid,
					Columns:    constraintColumns,
					Definition: constraintDefinition,
				},
				ReferencedTable: toolkit.LinkedTable{
					Schema: onTableSchema,
					Name:   onTableName,
					Oid:    onTableOid,
				},
			},
		})
	}

	return constraints, nil
}
