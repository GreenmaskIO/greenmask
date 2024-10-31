package context

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	transformersUtils "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// tableExistsQuery - map dump object to transformation config from yaml. This uses for validation and building
// configuration for Tables
type tableConfigMapping struct {
	entry  *entries.Table
	config *domains.Table
}

// entriesConfig - config for tables, sequences and blobs, that are used in the runtime context
type entriesConfig struct {
	tablesWithTransformers []*tableConfigMapping
	tables                 []*entries.Table
	sequences              []*entries.Sequence
	blobs                  *entries.Blobs
	// cachedRealTables - filtered list of tables that are not partitioned tables
	cachedRealTables []*entries.Table
}

func (ec *entriesConfig) Tables() []*entries.Table {
	if ec.cachedRealTables != nil {
		return ec.cachedRealTables
	}
	for _, t := range ec.tables {
		if t.RelKind == 'p' {
			continue
		}
		ec.cachedRealTables = append(ec.cachedRealTables, t)
	}
	return ec.cachedRealTables
}

func (ec *entriesConfig) Sequences() []*entries.Sequence {
	return ec.sequences
}

func (ec *entriesConfig) Blobs() *entries.Blobs {
	return ec.blobs
}

// ValidateAndBuildTableConfig - validates Tables, toolkit and their parameters. Builds config for Tables and returns
// ValidationWarnings that can be used for checking helpers in configuring and debugging transformation. Those
// may contain the schema affection warnings that would be useful for considering consistency
func validateAndBuildEntriesConfig(
	ctx context.Context, tx pgx.Tx, typeMap *pgtype.Map,
	cfg *domains.Dump, registry *transformersUtils.TransformerRegistry,
	version int, types []*toolkit.Type,
) (*entriesConfig, toolkit.ValidationWarnings, error) {
	var warnings toolkit.ValidationWarnings
	// Validate that the Tables in config exist in the database
	tableConfigExistsWarns, err := validateConfigTables(ctx, tx, cfg.Transformation)
	warnings = append(warnings, tableConfigExistsWarns...)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot validate Tables: %w", err)
	}
	if tableConfigExistsWarns.IsFatal() {
		return nil, tableConfigExistsWarns, nil
	}

	// Get list of entries (Tables, sequences, blobs) from the database
	tables, sequences, blobs, err := getDumpObjects(ctx, version, tx, &cfg.PgDumpOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot get Tables: %w", err)
	}

	// Assign settings to the Tables using config received
	//entriesWithTransformers := findTablesWithTransformers(cfg.Transformation, Tables)
	entriesWithTransformers, err := getTablesEntriesConfig(ctx, tx, cfg.Transformation, tables)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot get Tables entries config: %w", err)
	}
	// TODO:
	// 		Check if any has relkind = p
	// 		If yes, then find all children and remove them from entriesWithTransformers
	for _, cfgMapping := range entriesWithTransformers {
		// set subset conditions
		setSubsetConds(cfgMapping.entry, cfgMapping.config)
		// set query
		setQuery(cfgMapping.entry, cfgMapping.config)

		// Set global driver for the table
		driverWarnings, err := setGlobalDriverForTable(cfgMapping.entry, types)
		warnings = append(warnings, driverWarnings...)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"cannot set global driver for table %s.%s: %w",
				cfgMapping.entry.Schema, cfgMapping.entry.Name, err,
			)
		}
		enrichWarningsWithTableName(driverWarnings, cfgMapping.entry)
		if driverWarnings.IsFatal() {
			return nil, driverWarnings, nil
		}

		// Compile when condition and set to the table entry
		whenCondWarns := compileAndSetWhenCondForTable(cfgMapping.entry, cfgMapping.config)
		enrichWarningsWithTableName(driverWarnings, cfgMapping.entry)
		warnings = append(warnings, whenCondWarns...)
		if whenCondWarns.IsFatal() {
			return nil, whenCondWarns, nil
		}

		// Set table constraints
		if err := setTableConstraints(ctx, tx, cfgMapping.entry, version); err != nil {
			return nil, nil, fmt.Errorf(
				"cannot set table constraints for table %s.%s: %w",
				cfgMapping.entry.Schema, cfgMapping.entry.Name, err,
			)
		}

		// Set primary keys for the table
		if err := setTablePrimaryKeys(ctx, tx, cfgMapping.entry); err != nil {
			return nil, nil, fmt.Errorf(
				"cannot set primary keys for table %s.%s: %w",
				cfgMapping.entry.Schema, cfgMapping.entry.Name, err,
			)
		}

		// Set column type overrides
		setColumnTypeOverrides(cfgMapping.entry, cfgMapping.config, typeMap)

		// Set transformers for the table
		transformersInitWarns, err := initAndSetupTransformers(ctx, cfgMapping.entry, cfgMapping.config, registry)
		enrichWarningsWithTableName(transformersInitWarns, cfgMapping.entry)
		warnings = append(warnings, transformersInitWarns...)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"cannot initialise and set transformers for table %s.%s: %w",
				cfgMapping.entry.Schema, cfgMapping.entry.Name, err,
			)
		}
	}

	return &entriesConfig{
		tables:                 tables,
		tablesWithTransformers: entriesWithTransformers,
		sequences:              sequences,
		blobs:                  blobs,
	}, warnings, nil
}

// validateConfigTables - validates that the Tables in the config exist in the database. This function iterate through
// the Tables in the config and validates each of them
func validateConfigTables(
	ctx context.Context, tx pgx.Tx, cfg []*domains.Table,
) (toolkit.ValidationWarnings, error) {
	var totalWarnings toolkit.ValidationWarnings
	for _, t := range cfg {
		warnings, err := validateTableExists(ctx, tx, t)
		if err != nil {
			return nil, fmt.Errorf("cannot validate table %s.%s: %w", t.Name, t.Schema, err)
		}
		totalWarnings = append(totalWarnings, warnings...)
	}
	return totalWarnings, nil
}

// validateTableExists - validates that the table exists in the database. Returns validation warnings with error
// severity if the table does not exist
func validateTableExists(
	ctx context.Context, tx pgx.Tx, t *domains.Table,
) (toolkit.ValidationWarnings, error) {
	var exists bool
	var warnings toolkit.ValidationWarnings

	row := tx.QueryRow(ctx, tableExistsQuery, t.Schema, t.Name)
	if err := row.Scan(&exists); err != nil {
		return nil, fmt.Errorf("cannot scan table: %w", err)
	}

	if !exists {
		warnings = append(warnings, toolkit.NewValidationWarning().
			SetMsgf("table is not found").
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("Schema", t.Schema).
			AddMeta("TableName", t.Name),
		)
	}
	return warnings, nil
}

// findTablesWithTransformers - assigns settings from the config to the table entries. This function
// iterates through the Tables and do the following:
// 1. Compile when condition and set to the table entry
func findTablesWithTransformers(
	cfg []*domains.Table, tables []*entries.Table,
) []*tableConfigMapping {
	var entriesWithTransformers []*tableConfigMapping
	for _, entry := range tables {
		idx := slices.IndexFunc(cfg, func(table *domains.Table) bool {
			return (table.Name == entry.Name || fmt.Sprintf(`"%s"`, table.Name) == entry.Name) &&
				(table.Schema == entry.Schema || fmt.Sprintf(`"%s"`, table.Schema) == entry.Schema)
		})
		if idx != -1 {
			entriesWithTransformers = append(entriesWithTransformers, &tableConfigMapping{
				entry:  entry,
				config: cfg[idx],
			})
		}
	}
	return entriesWithTransformers
}

func getTablesEntriesConfig(
	ctx context.Context, tx pgx.Tx, cfg []*domains.Table, tables []*entries.Table,
) ([]*tableConfigMapping, error) {
	var res []*tableConfigMapping
	for _, tcm := range findTablesWithTransformers(cfg, tables) {
		if tcm.entry.RelKind != 'p' {
			res = append(res, tcm)
			continue
		}
		// If the table is partitioned, then we need to find all children and remove parent from the list
		if !tcm.config.ApplyForInherited {
			return nil, fmt.Errorf(
				"the table \"%s\".\"%s\" is partitioned use apply_for_inherited",
				tcm.entry.Schema, tcm.entry.Name,
			)
		}
		parts, err := findPartitionsOfPartitionedTable(ctx, tx, tcm.entry)
		if err != nil {
			return nil, fmt.Errorf(
				"cannot find partitions of the table %s.%s: %w",
				tcm.entry.Schema, tcm.entry.Name, err,
			)
		}
		for _, pt := range parts {
			idx := slices.IndexFunc(tables, func(table *entries.Table) bool {
				return table.Oid == pt
			})
			if idx == -1 {
				log.Debug().Msg("table might be excluded: table not found in selected tables")
				continue
			}
			e := tables[idx]
			e.Columns = tcm.entry.Columns
			res = append(res, &tableConfigMapping{
				entry:  e,
				config: tcm.config,
			})
		}
	}
	return res, nil
}

func findPartitionsOfPartitionedTable(ctx context.Context, tx pgx.Tx, t *entries.Table) ([]toolkit.Oid, error) {
	log.Debug().
		Str("TableSchema", t.Schema).
		Str("TableName", t.Name).
		Msg("table is partitioned: gathering all partitions and creating dumping tasks")
	// Get list of inherited Tables
	var parts []toolkit.Oid

	rows, err := tx.Query(ctx, TableGetChildPatsQuery, t.Oid)
	if err != nil {
		return nil, fmt.Errorf("error executing TableGetChildPatsQuery: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var pt toolkit.Oid
		if err = rows.Scan(&pt); err != nil {
			return nil, fmt.Errorf("error scanning TableGetChildPatsQuery: %w", err)
		}
		parts = append(parts, pt)
	}

	return parts, nil
}

func setSubsetConds(t *entries.Table, cfg *domains.Table) {
	t.SubsetConds = escapeSubsetConds(cfg.SubsetConds)
}

func setQuery(t *entries.Table, cfg *domains.Table) {
	t.Query = cfg.Query
}

func setGlobalDriverForTable(
	t *entries.Table, types []*toolkit.Type,
) (toolkit.ValidationWarnings, error) {
	driver, driverWarnings, err := toolkit.NewDriver(t.Table, types)
	if err != nil {
		return nil, fmt.Errorf("cannot initialise driver: %w", err)
	}
	if driverWarnings.IsFatal() {
		return driverWarnings, nil
	}
	t.Driver = driver
	return driverWarnings, nil
}

func compileAndSetWhenCondForTable(
	t *entries.Table, cfg *domains.Table,
) toolkit.ValidationWarnings {
	mata := map[string]any{
		"TableSchema": t.Schema,
		"TableName":   t.Name,
	}
	when, whenWarns := toolkit.NewWhenCond(cfg.When, t.Driver, mata)
	if whenWarns.IsFatal() {
		return whenWarns
	}
	t.When = when
	return whenWarns
}

func setTableConstraints(
	ctx context.Context, tx pgx.Tx, t *entries.Table, version int,
) (err error) {
	t.Constraints, err = getTableConstraints(ctx, tx, t.Oid, version)
	if err != nil {
		return fmt.Errorf("cannot get table constraints: %w", err)
	}
	return nil
}

func setTablePrimaryKeys(
	ctx context.Context, tx pgx.Tx, t *entries.Table,
) (err error) {
	t.PrimaryKey, err = getPrimaryKeyColumns(ctx, tx, t.Oid)
	if err != nil {
		return fmt.Errorf("unable to collect primary key columns: %w", err)
	}
	return nil
}

func setColumnTypeOverrides(
	t *entries.Table, cfg *domains.Table, typeMap *pgtype.Map,
) {
	if cfg.ColumnsTypeOverride == nil {
		return
	}
	for _, c := range t.Columns {
		overridingType, ok := cfg.ColumnsTypeOverride[c.Name]
		if ok {
			c.OverrideType(
				overridingType,
				getTypeOidByName(overridingType, typeMap),
				getTypeSizeByeName(overridingType),
			)
		}
	}
}

func enrichWarningsWithTableName(warns toolkit.ValidationWarnings, t *entries.Table) {
	for _, w := range warns {
		w.AddMeta("SchemaName", t.Schema).
			AddMeta("TableName", t.Name)
	}
}

func enrichWarningsWithTransformerName(warns toolkit.ValidationWarnings, n string) {
	for _, w := range warns {
		w.AddMeta("TransformerName", n)
	}
}

func initAndSetupTransformers(
	ctx context.Context, t *entries.Table, cfg *domains.Table, r *transformersUtils.TransformerRegistry,
) (toolkit.ValidationWarnings, error) {
	var warnings toolkit.ValidationWarnings
	if len(cfg.Transformers) == 0 {
		return nil, nil
	}
	for _, tc := range cfg.Transformers {
		transformationCtx, initWarnings, err := initTransformer(ctx, t.Driver, tc, r)
		enrichWarningsWithTransformerName(initWarnings, tc.Name)
		if err != nil {
			return initWarnings, err
		}
		warnings = append(warnings, initWarnings...)
		t.TransformersContext = append(t.TransformersContext, transformationCtx)
	}
	return warnings, nil
}
