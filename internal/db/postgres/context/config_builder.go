package context

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/subset"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers"
	transformersUtils "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	columnParameterName = "column"
	engineParameterName = "engine"
)

// transformersMapping - map dump object to transformation config from yaml. This uses for validation and building
// configuration for Tables
type transformersMapping struct {
	entry      *entries.Table
	columnName string
	attNum     int
	cfg        *domains.TransformerConfig
}

// tableExistsQuery - map dump object to transformation config from yaml. This uses for validation and building
// configuration for Tables
type tableConfigMapping struct {
	entry  *entries.Table
	config *domains.Table
}

func (tcm *tableConfigMapping) hasTransformerWithApplyForReferences() bool {
	for _, tr := range tcm.config.Transformers {
		if tr.ApplyForReferences {
			return true
		}
	}
	return false
}

// ValidateAndBuildTableConfig - validates Tables, toolkit and their parameters. Builds config for Tables and returns
// ValidationWarnings that can be used for checking helpers in configuring and debugging transformation. Those
// may contain the schema affection warnings that would be useful for considering consistency
func validateAndBuildEntriesConfig(
	ctx context.Context, tx pgx.Tx, entries []*entries.Table, typeMap *pgtype.Map,
	cfg *domains.Dump, r *transformersUtils.TransformerRegistry,
	version int, types []*toolkit.Type, graph *subset.Graph,
) (toolkit.ValidationWarnings, error) {
	var warnings toolkit.ValidationWarnings
	// Validate that the Tables in config exist in the database
	tableConfigExistsWarns, err := validateConfigTables(ctx, tx, cfg.Transformation)
	warnings = append(warnings, tableConfigExistsWarns...)
	if err != nil {
		return nil, fmt.Errorf("cannot validate Tables: %w", err)
	}
	if tableConfigExistsWarns.IsFatal() {
		return tableConfigExistsWarns, nil
	}

	// Assign settings to the Tables using config received
	entriesWithTransformers, setConfigWarns, err := setConfigToEntries(ctx, tx, cfg.Transformation, entries, graph, r)
	if err != nil {
		return nil, fmt.Errorf("cannot get Tables entries config: %w", err)
	}
	warnings = append(warnings, setConfigWarns...)
	for _, cfgMapping := range entriesWithTransformers {
		// set subset conditions
		setSubsetConds(cfgMapping.entry, cfgMapping.config)
		// set query
		setQuery(cfgMapping.entry, cfgMapping.config)

		// Set global driver for the table
		driverWarnings, err := setGlobalDriverForTable(cfgMapping.entry, types)
		warnings = append(warnings, driverWarnings...)
		if err != nil {
			return nil, fmt.Errorf(
				"cannot set global driver for table %s.%s: %w",
				cfgMapping.entry.Schema, cfgMapping.entry.Name, err,
			)
		}
		enrichWarningsWithTableName(driverWarnings, cfgMapping.entry)
		if driverWarnings.IsFatal() {
			return driverWarnings, nil
		}

		// Compile when condition and set to the table entry
		whenCondWarns := compileAndSetWhenCondForTable(cfgMapping.entry, cfgMapping.config)
		enrichWarningsWithTableName(driverWarnings, cfgMapping.entry)
		warnings = append(warnings, whenCondWarns...)
		if whenCondWarns.IsFatal() {
			return whenCondWarns, nil
		}

		// Set table constraints
		if err := setTableConstraints(ctx, tx, cfgMapping.entry, version); err != nil {
			return nil, fmt.Errorf(
				"cannot set table constraints for table %s.%s: %w",
				cfgMapping.entry.Schema, cfgMapping.entry.Name, err,
			)
		}

		// Set primary keys for the table
		if err := setTablePrimaryKeys(ctx, tx, cfgMapping.entry); err != nil {
			return nil, fmt.Errorf(
				"cannot set primary keys for table %s.%s: %w",
				cfgMapping.entry.Schema, cfgMapping.entry.Name, err,
			)
		}

		// Set column type overrides
		setColumnTypeOverrides(cfgMapping.entry, cfgMapping.config, typeMap)

		// Set transformers for the table
		transformersInitWarns, err := initAndSetupTransformers(ctx, cfgMapping.entry, cfgMapping.config, r)
		enrichWarningsWithTableName(transformersInitWarns, cfgMapping.entry)
		warnings = append(warnings, transformersInitWarns...)
		if err != nil {
			return nil, fmt.Errorf(
				"cannot initialise and set transformers for table %s.%s: %w",
				cfgMapping.entry.Schema, cfgMapping.entry.Name, err,
			)
		}
	}

	return warnings, nil
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

// findTablesWithTransformers - finds Tables with transformers in the config and returns them as a slice of
// tableConfigMapping
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

func setConfigToEntries(
	ctx context.Context, tx pgx.Tx, cfg []*domains.Table, tables []*entries.Table, g *subset.Graph,
	r *transformersUtils.TransformerRegistry,
) ([]*tableConfigMapping, toolkit.ValidationWarnings, error) {
	var res []*tableConfigMapping
	var warnings toolkit.ValidationWarnings
	for _, tcm := range findTablesWithTransformers(cfg, tables) {
		if tcm.hasTransformerWithApplyForReferences() {
			// If table has transformer with apply_for_references, then we need to find all reference tables
			// and add them to the list
			ok, checkWarns := checkApplyForReferenceMetRequirements(tcm, r)
			if !ok {
				warnings = append(warnings, checkWarns...)
				continue
			}
			refTables := getRefTables(tcm.entry, tcm.config, g, cfg)
			res = append(res, refTables...)
		}
		if tcm.entry.RelKind != 'p' {
			// If table is not partitioned, simply append it to the result
			res = append(res, tcm)
			continue
		}
		// If the table is partitioned, then we need to find all children and remove parent from the list
		if !tcm.config.ApplyForInherited {
			warnings = append(warnings, toolkit.NewValidationWarning().
				SetMsg("the table is partitioned use apply_for_inherited").
				AddMeta("SchemaName", tcm.entry.Schema).
				AddMeta("TableName", tcm.entry.Name).
				SetSeverity(toolkit.ErrorValidationSeverity),
			)
			continue
		}

		parts, err := findPartitionsOfPartitionedTable(ctx, tx, tcm.entry)
		if err != nil {
			return nil, nil, fmt.Errorf(
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
			e.RootPtName = tcm.entry.Name
			e.RootPtSchema = tcm.entry.Schema
			e.RootPtOid = tcm.entry.Oid
			e.Columns = tcm.entry.Columns
			res = append(res, &tableConfigMapping{
				entry:  e,
				config: tcm.config,
			})
		}
	}
	return res, warnings, nil
}

func getRefTables(
	rootTable *entries.Table, rootTableCfg *domains.Table, graph *subset.Graph, allTrans []*domains.Table,
) []*tableConfigMapping {
	var res []*tableConfigMapping
	rootTrans := collectRootTransformers(rootTable, rootTableCfg)

	// Start DFS traversal from the root table
	buildRefsWithEndToEndDfs(
		rootTable, rootTableCfg, rootTrans, graph, allTrans, &res, false,
	)

	return res
}

// buildRefsWithEndToEndDfs performs depth-first search to apply transformations to child tables
// based on the root transformers mapping and graph structure, avoiding cycles
func buildRefsWithEndToEndDfs(
	table *entries.Table, rootTableCfg *domains.Table, rootTrans []*transformersMapping,
	graph *subset.Graph, allTrans []*domains.Table,
	res *[]*tableConfigMapping, checkEndToEnd bool) {
	//tableKey := fmt.Sprintf("%s.%s", table.Schema, table.Name)
	//if visited[tableKey] {
	//	return
	//}
	//visited[tableKey] = true
	//defer func() { delete(visited, tableKey) }() // unmark after recursion

	rg := graph.ReversedGraph()
	tableIdx := findTableIndex(graph, table)
	if tableIdx == -1 {
		log.Warn().
			Str("SchemaName", table.Schema).
			Str("TableName", table.Name).
			Msg("transformer inheritance for ref: cannot find table in the graph: table will be ignored")
		return
	}

	for _, r := range rg[tableIdx] {
		// Check for end-to-end PK-FK relationship only if it's beyond the first table
		if checkEndToEnd && !isEndToEndPKFK(graph, r.From().Table()) {
			continue
		}
		processReference(r, rootTableCfg, rootTrans, allTrans, res)
		// Recursively call DFS on child reference, setting checkEndToEnd to true after the first level
		buildRefsWithEndToEndDfs(
			r.To().Table(), rootTableCfg, rootTrans, graph, allTrans, res, true,
		)
	}
}

// collectRootTransformers gathers all transformers in the root table's configuration
func collectRootTransformers(rootTable *entries.Table, rootTableCfg *domains.Table) []*transformersMapping {
	var rootTransformersMapping []*transformersMapping
	for _, tr := range rootTableCfg.Transformers {
		if !tr.ApplyForReferences || string(tr.Params[engineParameterName]) != "hash" {
			continue
		}
		idx := slices.Index(rootTable.PrimaryKey, string(tr.Params[columnParameterName]))
		if idx == -1 {
			continue
		}
		rootTransformersMapping = append(rootTransformersMapping, &transformersMapping{
			entry:      rootTable,
			columnName: string(tr.Params[columnParameterName]),
			attNum:     idx,
			cfg:        tr,
		})
	}
	return rootTransformersMapping
}

// findTableIndex locates the index of a table in the graph by name and schema
func findTableIndex(graph *subset.Graph, table *entries.Table) int {
	return slices.IndexFunc(graph.GetTables(), func(t *entries.Table) bool {
		return (table.Name == t.Name || fmt.Sprintf(`"%s"`, table.Name) == t.Name) &&
			(table.Schema == t.Schema || fmt.Sprintf(`"%s"`, table.Schema) == t.Schema)
	})
}

// processReference applies transformers to the reference table if it matches criteria
// and recursively calls buildRefsWithEndToEndDfs on the child references
func processReference(
	r *subset.Edge, rootTableCfg *domains.Table, rootTrans []*transformersMapping,
	allTrans []*domains.Table, res *[]*tableConfigMapping,
) {
	for _, rootTr := range rootTrans {
		// Get the primary key column name of the root table
		fkKeys := r.To().Keys()
		refColName := fkKeys[rootTr.attNum].Name

		found, conf := checkTransformerAlreadyExists(
			allTrans, r.To().Table().Schema, r.To().Table().Name, rootTr.cfg.Name, refColName,
		)
		if found {
			log.Info().
				Str("TransformerName", rootTr.cfg.Name).
				Str("ParentTableSchema", rootTableCfg.Schema).
				Str("ParentTableName", rootTableCfg.Name).
				Str("ChildTableSchema", r.To().Table().Schema).
				Str("ChildTableName", r.To().Table().Name).
				Str("ChildColumnName", refColName).
				Any("TransformerConfig", conf).
				Msg("skipping apply transformer for reference: found manually configured transformer")
			continue
		}

		trConf := rootTr.cfg.Clone()
		trConf.Params["column"] = toolkit.ParamsValue(refColName)

		colTypeOverride := getColumnTypeOverride(rootTableCfg, rootTr.columnName)
		addTransformerToReferenceTable(r, trConf, colTypeOverride, res)
	}
}

// addTransformerToReferenceTable adds the transformer configuration to the reference table in the results
func addTransformerToReferenceTable(
	r *subset.Edge, trConf *domains.TransformerConfig,
	colTypeOverride map[string]string, res *[]*tableConfigMapping,
) {
	refTableIdx := slices.IndexFunc(*res, func(tcm *tableConfigMapping) bool {
		return tcm.entry.Name == r.To().Table().Name && tcm.entry.Schema == r.To().Table().Schema
	})
	if refTableIdx != -1 {
		(*res)[refTableIdx].config.Transformers = append((*res)[refTableIdx].config.Transformers, trConf)
	} else {
		*res = append(*res, &tableConfigMapping{
			entry: r.To().Table(),
			config: &domains.Table{
				Schema:              r.To().Table().Schema,
				Name:                r.To().Table().Name,
				Transformers:        []*domains.TransformerConfig{trConf},
				ColumnsTypeOverride: colTypeOverride,
			},
		})
	}
}

// getColumnTypeOverride retrieves column type overrides for foreign key columns, if specified
func getColumnTypeOverride(rootTableCfg *domains.Table, columnName string) map[string]string {
	colTypeOverride := make(map[string]string)
	if rootTableCfg.ColumnsTypeOverride != nil && rootTableCfg.ColumnsTypeOverride[columnName] != "" {
		colTypeOverride[columnName] = rootTableCfg.ColumnsTypeOverride[columnName]
	}
	return colTypeOverride
}

// isEndToEndPKFK checks if a table has PK and FK on the same columns (end-to-end identifier) using the graph
func isEndToEndPKFK(graph *subset.Graph, table *entries.Table) bool {
	// Get all references of the table using the graph
	//references := graph.GetReferencesForTable(table)
	idx := slices.IndexFunc(graph.Tables(), func(t *entries.Table) bool {
		return t.Name == table.Name && t.Schema == table.Schema
	})
	rg := graph.ReversedGraph()
	var foundInFK bool
	for _, ref := range rg[idx] {
		for _, fkColName := range ref.To().Keys() {
			for _, pkColName := range ref.To().Table().PrimaryKey {
				if pkColName == fkColName.Name {
					foundInFK = true
					break
				}
			}
			if foundInFK {
				break
			}
		}
	}
	return foundInFK
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

func setTablePrimaryKeys(ctx context.Context, tx pgx.Tx, t *entries.Table,
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

func initAndSetupTransformers(ctx context.Context, t *entries.Table, cfg *domains.Table, r *transformersUtils.TransformerRegistry,
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

func checkApplyForReferenceMetRequirements(
	tcm *tableConfigMapping, r *transformersUtils.TransformerRegistry,
) (bool, toolkit.ValidationWarnings) {
	warnings := toolkit.ValidationWarnings{}
	for _, tr := range tcm.config.Transformers {
		allowed, w := isTransformerAllowedToApplyForReferences(tr, r)
		if !allowed {
			warnings = append(warnings, w...)
		}
	}
	return !warnings.IsFatal(), warnings
}

// isTransformerAllowedToApplyForReferences - checks if the transformer is allowed to apply for references
// and if the engine parameter is hash and required
func isTransformerAllowedToApplyForReferences(
	cfg *domains.TransformerConfig, r *transformersUtils.TransformerRegistry,
) (bool, toolkit.ValidationWarnings) {
	td, ok := r.Get(cfg.Name)
	if !ok {
		return false, toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetMsg("transformer not found").
				AddMeta("TransformerName", cfg.Name).
				SetSeverity(toolkit.ErrorValidationSeverity),
		}
	}
	allowApplyForReferenced, ok := td.Properties.GetMeta(transformers.AllowApplyForReferenced)
	if !ok || !allowApplyForReferenced.(bool) {
		return false, toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetMsg(
					"cannot apply transformer for references: transformer does not support apply for references",
				).
				AddMeta("TransformerName", cfg.Name).
				SetSeverity(toolkit.ErrorValidationSeverity),
		}
	}
	requireHashEngineParameter, ok := td.Properties.GetMeta(transformers.RequireHashEngineParameter)
	if !ok {
		return false, nil
	}
	if !requireHashEngineParameter.(bool) {
		return true, nil
	}
	if string(cfg.Params[engineParameterName]) != transformers.HashEngineParameterName {
		return false, toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetMsg("cannot apply transformer for references: engine parameter is not hash").
				AddMeta("TransformerName", cfg.Name).
				SetSeverity(toolkit.ErrorValidationSeverity),
		}
	}
	return true, nil
}

func checkTransformerAlreadyExists(
	conf []*domains.Table, schemaName, tableName, tranName, tColumn string,
) (bool, *domains.TransformerConfig) {
	for _, c := range conf {
		if c.Name == tableName && c.Schema == schemaName {
			for _, tr := range c.Transformers {
				if tr.Name == tranName && string(tr.Params[columnParameterName]) == tColumn {
					return true, tr
				}
			}
		}
	}
	return false, nil
}
