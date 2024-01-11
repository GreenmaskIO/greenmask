// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// ValidateAndBuildTableConfig - validates tables, toolkit and their parameters. Builds config for tables and returns
// ValidationWarnings that can be used for checking helpers in configuring and debugging transformation. Those
// may contain the schema affection warnings that would be useful for considering consistency
func validateAndBuildTablesConfig(
	ctx context.Context, tx pgx.Tx, typeMap *pgtype.Map,
	cfg []*domains.Table, registry *transformersUtils.TransformerRegistry,
	version int, types []*toolkit.Type,
) (map[toolkit.Oid]*dump.Table, toolkit.ValidationWarnings, error) {
	result := make(map[toolkit.Oid]*dump.Table, len(cfg))
	var warnings toolkit.ValidationWarnings

	for _, tableCfg := range cfg {
		tables, tableWarnings, err := getTable(ctx, tx, tableCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot build tables from config: %w", err)
		}
		warnings = append(warnings, tableWarnings...)
		if len(tableWarnings) > 0 {
			continue
		}
		for _, table := range tables {
			// Assign tables constraints
			constraints, err := getTableConstraints(ctx, tx, table.Oid, version)
			if err != nil {
				return nil, nil, err
			}
			table.Constraints = constraints

			// Assigning overridden column types for driver initialization
			if tableCfg.ColumnsTypeOverride != nil {
				for _, c := range table.Columns {
					overridingType, ok := tableCfg.ColumnsTypeOverride[c.Name]
					if ok {
						c.OverriddenTypeName = overridingType
					}
				}
			}

			// Assign columns and transformersMap if were found
			columns, err := getColumnsConfig(ctx, tx, table.Oid)
			if err != nil {
				return nil, nil, err
			}
			table.Columns = columns

			driver, driverWarnings, err := toolkit.NewDriver(table.Table, types)
			if err != nil {
				return nil, nil, fmt.Errorf("unnable to initialise driver: %w", err)
			}
			table.Driver = driver

			if len(driverWarnings) > 0 {
				for _, w := range driverWarnings {
					w.AddMeta("SchemaName", table.Schema).
						AddMeta("TableName", table.Name)
				}
				warnings = append(warnings, driverWarnings...)
			}

			// InitTransformation toolkit
			if len(tableCfg.Transformers) > 0 {
				for _, tc := range tableCfg.Transformers {
					transformer, initWarnings, err := initTransformer(ctx, driver, tc, registry, types)
					if len(initWarnings) > 0 {
						for _, w := range initWarnings {
							// Enriching the tables context into meta
							w.AddMeta("SchemaName", table.Schema).
								AddMeta("TableName", table.Name).
								AddMeta("TransformerName", tc.Name)

						}
					}
					// Not only errors might be in driver initialization but also a warnings that's why we have to add
					// append medata to validation warnings and the check error and return error with warnings
					if err != nil {
						return nil, warnings, err
					}
					warnings = append(warnings, initWarnings...)
					table.Transformers = append(table.Transformers, transformer)
				}
			}

			result[table.Oid] = table
		}

	}

	return result, warnings, nil
}

func getTable(ctx context.Context, tx pgx.Tx, t *domains.Table) ([]*dump.Table, toolkit.ValidationWarnings, error) {
	table := &dump.Table{
		Table: &toolkit.Table{},
	}
	var warnings toolkit.ValidationWarnings
	var tables []*dump.Table

	row := tx.QueryRow(ctx, TableSearchQuery, t.Schema, t.Name)
	err := row.Scan(&table.Oid, &table.Schema, &table.Name, &table.Owner, &table.RelKind,
		&table.RootPtSchema, &table.RootPtName, &table.RootOid,
	)

	if err != nil && errors.Is(err, pgx.ErrNoRows) {
		warnings = append(warnings, toolkit.NewValidationWarning().
			SetMsgf("table is not found").
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("Schema", t.Schema).
			AddMeta("TableName", t.Name),
		)
	} else if err != nil {
		return nil, nil, fmt.Errorf("cannot scan table: %w", err)
	}

	if t.Query != "" && table.RelKind == 'p' {
		return nil, nil, fmt.Errorf("cannot aply custom query on partitioned table \"%s\".\"%s\": is not supported", table.Schema, table.Name)
	}
	table.Query = t.Query

	if table.RelKind == 'p' {
		if !t.ApplyForInherited {
			return nil, nil, fmt.Errorf("the table \"%s\".\"%s\" is partitioned use apply_for_inherited", table.Schema, table.Name)
		}
		log.Debug().
			Str("TableSchema", table.Schema).
			Str("TableName", table.Name).
			Msg("table is partitioned: gathering all partitions and creating dumping tasks")
		// Get list of inherited tables
		var parts []*dump.Table

		rows, err := tx.Query(ctx, TableGetChildPatsQuery, table.Oid)
		if err != nil {
			return nil, nil, fmt.Errorf("error executing TableGetChildPatsQuery: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			pt := &dump.Table{
				Table:        &toolkit.Table{},
				RootPtSchema: table.Schema,
				RootPtName:   table.Name,
				RootOid:      table.Oid,
			}
			if err = rows.Scan(&pt.Oid, &pt.Schema, &pt.Name); err != nil {
				return nil, nil, fmt.Errorf("error scanning TableGetChildPatsQuery: %w", err)
			}
			parts = append(parts, pt)
		}

		for _, pt := range parts {
			row = tx.QueryRow(ctx, TableSearchQuery, pt.Schema, pt.Name)
			err = row.Scan(&pt.Oid, &pt.Schema, &pt.Name, &pt.Owner, &pt.RelKind,
				&pt.RootPtSchema, &pt.RootPtName, &pt.RootOid,
			)
			if err != nil {
				return nil, nil, fmt.Errorf("error scanning TableSearchQuery for parts: %w", err)
			}
		}

		tables = append(tables, parts...)

	} else {
		tables = append(tables, table)
	}

	return tables, warnings, nil
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
