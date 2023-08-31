package config_builder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/config"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/dump"
	toolkit "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
)

type RuntimeConfig struct {
	Types  []*toolkit.Type
	Tables map[toolkit.Oid]*dump.Table
}

// ValidateAndBuildRuntimeConfig - validates tables, transformers and their parameters. Builds config for tables and returns
// ValidationWarnings that can be used for checking helpers in configuring and debugging transformation. Those
// may contain the schema affection warnings that would be useful for considering consistency
func ValidateAndBuildRuntimeConfig(
	ctx context.Context, tx pgx.Tx, tableConfig []*config.Table,
) (*RuntimeConfig, toolkit.ValidationWarnings, error) {
	transformersMap, err := buildTransformersMap()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot build transformer map: %w", err)
	}

	tables := make(map[toolkit.Oid]*dump.Table, len(tableConfig))
	var warnings toolkit.ValidationWarnings

	for _, t := range tableConfig {
		table, tableWarnings, err := getTable(ctx, tx, t.Schema, t.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot build table from config: %w", err)
		}
		warnings = append(warnings, tableWarnings...)
		if len(tableWarnings) > 0 {
			continue
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
		if len(t.Transformers) > 0 {
			for _, tc := range t.Transformers {
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

	types, err := getCustomTypesUsedInTables(ctx, tx)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot discover types: %w", err)
	}

	return &RuntimeConfig{
		Types:  types,
		Tables: tables,
	}, warnings, nil
}
