// Copyright 2025 Greenmask
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

package introspect

import (
	"context"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
)

var (
	errNoKeysFound                 = errors.New("no keys found")
	errCannotMatchTypeToVirtualOID = errors.New("cannot match type to virtual OID")
	errNoSchemasFound              = errors.New("no schemas/databases found according to the filters")
)

type options interface {
	GetIncludedTables() []string
	GetExcludedTables() []string
	GetExcludedSchemas() []string
	GetIncludedSchemas() []string
	GetExcludedDatabases() []string
	GetIncludedDatabases() []string
	GetIncludedTableData() []string
	GetExcludedTableData() []string
	GetIncludedTableDefinitions() []string
	GetExcludedTableDefinitions() []string
}

type Introspector struct {
	tables                    []mysqlmodels.Table
	excludedTables            []mysqlmodels.Table
	allSchemas                []string
	explicitlyIncludedSchemas []string
	explicitlyExcludedSchemas []string
	opt                       options
	tm                        *objectMatcher

	hasIncTables  bool
	hasExcTables  bool
	hasIncData    bool
	hasExcData    bool
	hasIncSchemas bool
	hasExcSchemas bool
}

func NewIntrospector(opt options) (*Introspector, error) {
	tm, err := newObjectMatcher(opt)
	if err != nil {
		return nil, fmt.Errorf("create table matcher: %w", err)
	}

	return &Introspector{
		opt:           opt,
		tm:            tm,
		hasIncSchemas: len(opt.GetIncludedSchemas()) > 0 || len(opt.GetIncludedDatabases()) > 0,
		hasExcSchemas: len(opt.GetExcludedSchemas()) > 0 || len(opt.GetExcludedDatabases()) > 0,
		hasIncTables:  len(opt.GetIncludedTables()) > 0 || len(opt.GetIncludedTableDefinitions()) > 0,
		hasExcTables:  len(opt.GetExcludedTables()) > 0 || len(opt.GetExcludedTableDefinitions()) > 0,
		hasIncData:    len(opt.GetIncludedTableData()) > 0,
		hasExcData:    len(opt.GetExcludedTableData()) > 0,
	}, nil
}

func (i *Introspector) GetTables() []mysqlmodels.Table {
	return i.tables
}

// GetSchemaRelatedSettings - generates the GenericSchemaRelatedSettings based on the introspected tables and the
// user's filter settings. This is required to implement a bridge between vendor clike tools (e.g. mysqldump) and
// greenmask's internal filtering and dumping logic.
func (i *Introspector) GetSchemaRelatedSettings() commonmodels.MysqlDumpRelatedSettings {
	res := commonmodels.MysqlDumpRelatedSettings{
		ExcludeTables:    make(map[string][]string),
		IncludeTables:    make(map[string][]string),
		ExcludeTableData: make(map[string][]string),
		IncludeTableData: make(map[string][]string),
	}

	// has* variables are used to determine the user's explicit filter intent.
	// We use them to selectively populate inclusion and exclusion lists to avoid redundancy.
	// For example:
	// - If a user only sets `include-table: [t1]`, we only populate res.IncludeTables with t1.
	//   Providing res.ExcludeTables with all other tables in the database would be redundant
	//   and could lead to extremely long mysqldump commands.
	// - If a user ONLY sets `exclude-table: [t1]`, we only populate res.ExcludeTables.
	// - If both are set (mixed mode), we populate both to respect the explicit filters.
	// This sparse population helps generating more efficient CLI arguments for tools like mysqldump.
	// This is required to avoid generating extremely long mysqldump commands in cases when user sets only a
	// few inclusions (e.g. include-table: [t1]) and there are hundreds of other tables in the
	// database that should be excluded.

	for _, s := range i.allSchemas {
		if i.tm.MatchSchemaIsAllowed(s) {
			res.AllowedSchemas = append(res.AllowedSchemas, s)
		}
	}

	for _, t := range i.tables {
		if t.NeedDumpSchema {
			// In inclusion mode or basic mode, list the included tables.
			if i.hasIncTables || !i.hasExcTables {
				res.IncludeTables[t.Schema] = append(res.IncludeTables[t.Schema], t.Name)
			}
		} else if i.hasExcTables {
			// In exclusion mode, list the explicitly excluded tables.
			res.ExcludeTables[t.Schema] = append(res.ExcludeTables[t.Schema], t.Name)
		}

		if t.NeedDumpData {
			// Default behavior for data is inclusion unless explicit exclusions exist.
			if i.hasIncData || !i.hasExcData {
				res.IncludeTableData[t.Schema] = append(res.IncludeTableData[t.Schema], t.Name)
			}
		} else if i.hasExcData {
			// If explicit data exclusions exist, list them.
			res.ExcludeTableData[t.Schema] = append(res.ExcludeTableData[t.Schema], t.Name)
		}
	}

	for _, t := range i.excludedTables {
		// We avoid passing implicitly filtered schemas to the CLI unless they are explicitly excluded
		// by the user to avoid generating extremely long mysqldump commands.
		// If it's a system schema that is not allowed, it means it was implicitly filtered.
		if isSystemSchema(t.Schema) && !i.tm.MatchSchemaIsAllowed(t.Schema) {
			continue
		}
		// Tables that were completely excluded (neither schema nor data) are only
		// added to exclusion lists if the user is using an exclusion-based filter.
		if i.hasExcTables {
			res.ExcludeTables[t.Schema] = append(res.ExcludeTables[t.Schema], t.Name)
		}
		if i.hasExcData {
			res.ExcludeTableData[t.Schema] = append(res.ExcludeTableData[t.Schema], t.Name)
		}
	}

	return res
}

func (i *Introspector) GetCommonTables() []commonmodels.Table {
	tables := make([]commonmodels.Table, len(i.tables))
	for idx, table := range i.tables {
		tables[idx] = table.ToCommonTable()
	}
	return tables
}

func (i *Introspector) GetMatchedDatabases() []string {
	var res []string
	for _, s := range i.allSchemas {
		if i.tm.MatchSchemaIsAllowed(s) {
			res = append(res, s)
		}
	}
	return res
}

// Introspect - introspects the mysql instance provided. It received a transaction
// because the data have to be consistent.
func (i *Introspector) Introspect(ctx context.Context, tx interfaces.DB) error {
	schemas, err := i.getSchemas(ctx, tx)
	if err != nil {
		return fmt.Errorf("get schemas: %w", err)
	}
	i.allSchemas = schemas

	tables, err := i.getTables(ctx, tx)
	if err != nil {
		return fmt.Errorf("introspect tables: %w", err)
	}

	var tableIDSeq int
	for _, t := range tables {
		// determine weather we need to dump schema and data for the table based on the filters.
		t.NeedDumpSchema = i.tm.MatchNeedDumpSchema(t.Schema, t.Name)
		t.NeedDumpData = i.tm.MatchNeedDumpData(t.Schema, t.Name)

		if !t.NeedDumpSchema && !t.NeedDumpData {
			// We will fill all tables that has been skipped by filters to i.excludedTables, and
			// they might be used for generating ignore-table list in the generic schema settings
			// (GetSchemaRelatedSettings method)
			log.Ctx(ctx).Debug().
				Str("Schema", t.Schema).
				Str("Table", t.Name).
				Msg("table excluded by filters")
			i.excludedTables = append(i.excludedTables, t)
			continue
		}

		t.ID = tableIDSeq
		tableIDSeq++

		log.Ctx(ctx).Debug().
			Int("ID", t.ID).
			Bool("NeedDumpSchema", t.NeedDumpSchema).
			Bool("NeedDumpData", t.NeedDumpData).
			Str("Schema", t.Schema).
			Str("Table", t.Name).
			Msg("including table in introspection")

		columns, err := i.getColumns(ctx, tx, t.Schema, t.Name)
		if err != nil {
			return fmt.Errorf("introspect columns for table %s.%s: %w", t.Schema, t.Name, err)
		}
		t.SetColumns(columns)

		pkColumns, err := i.getPrimaryKey(ctx, tx, t.Schema, t.Name)
		if err != nil {
			return fmt.Errorf("introspect primary key for table %s.%s: %w", t.Schema, t.Name, err)
		}
		t.SetPrimaryKey(pkColumns)

		fks, err := i.getForeignKeys(ctx, tx, t.Schema, t.Name)
		if err != nil {
			return fmt.Errorf("introspect foreign keys for table %s.%s: %w", t.Schema, t.Name, err)
		}
		t.SetReferences(fks)

		i.tables = append(i.tables, t)
	}

	return nil
}

// getTables - get all tables from the database excluding system tables
func (i *Introspector) getTables(ctx context.Context, tx interfaces.DB) ([]mysqlmodels.Table, error) {
	// Build the query to fetch all base tables from all available schemas
	query := `
		select t.TABLE_SCHEMA as schema_name, 
			   t.table_name   as table_name,
			   t.DATA_LENGTH  AS table_size_mb
		from information_schema.tables t
		WHERE t.TABLE_TYPE = 'BASE TABLE'
		  AND t.TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'sys')
	`

	var args []interface{}
	if i.hasIncSchemas {
		query += fmt.Sprintf(" AND t.TABLE_SCHEMA IN (%s) ", repeatPlaceholder(len(i.explicitlyIncludedSchemas)))
		for _, s := range i.explicitlyIncludedSchemas {
			args = append(args, s)
		}
	}
	if i.hasExcSchemas {
		query += fmt.Sprintf(" AND t.TABLE_SCHEMA NOT IN (%s) ", repeatPlaceholder(len(i.explicitlyExcludedSchemas)))
		for _, s := range i.explicitlyExcludedSchemas {
			args = append(args, s)
		}
	}
	query += " ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;"

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error closing table rows")
		}
	}()

	var tables []mysqlmodels.Table
	schemaTableCount := make(map[string]int)

	for rows.Next() {
		var (
			tableName, tableSchema string
			tableSize              *int64
		)
		if err := rows.Scan(&tableSchema, &tableName, &tableSize); err != nil {
			return nil, err
		}

		tables = append(tables, mysqlmodels.Table{
			Name:   tableName,
			Schema: tableSchema,
			Size:   tableSize,
		})
		schemaTableCount[tableSchema]++
	}

	for schema, count := range schemaTableCount {
		log.Ctx(ctx).Debug().
			Str("Schema", schema).
			Int("TableCount", count).
			Msg("fetched tables from schema")
	}

	log.Ctx(ctx).Debug().
		Int("TotalTables", len(tables)).
		Msg("completed table introspection")

	return tables, nil
}

// getSchemas - get all possible schemas and databases
func (i *Introspector) getSchemas(ctx context.Context, tx interfaces.DB) ([]string, error) {
	query := `
		SELECT 
			SCHEMA_NAME 
		FROM information_schema.schemata 
		WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'sys')
	`
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("execute schemas query: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error closing schemas rows")
		}
	}()

	var schemas []string
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return nil, fmt.Errorf("scan schema name: %w", err)
		}
		schemas = append(schemas, schemaName)
	}

	log.Ctx(ctx).Debug().
		Int("SchemasCount", len(i.allSchemas)).
		Msg("fetched all available schemas")

	var anySchemaMatched bool

	for _, s := range schemas {
		if i.tm.MatchSchemaIsAllowed(s) {
			anySchemaMatched = true
		}
		if i.hasIncSchemas && i.tm.isSchemaIncluded(s) {
			i.explicitlyIncludedSchemas = append(i.explicitlyIncludedSchemas, s)
		}
		if i.hasExcSchemas && i.tm.isSchemaExcluded(s) {
			i.explicitlyExcludedSchemas = append(i.explicitlyExcludedSchemas, s)
		}
	}

	if !anySchemaMatched {
		return nil, errNoSchemasFound
	}
	return schemas, nil
}

func getTypeOID(columnType string, dataType *string) (commonmodels.VirtualOID, error) {
	typeOID, ok := dbmsdriver.TypeNameToVirtualOid[columnType]
	if ok {
		return typeOID, nil
	}
	// If not found, try to use fallback using dataType if provided.
	if dataType == nil {
		return 0, fmt.Errorf("match type OID for %s: %w", columnType, errCannotMatchTypeToVirtualOID)
	}
	typeOID, ok = dbmsdriver.TypeNameToVirtualOid[*dataType]
	if ok {
		return typeOID, nil
	}
	return 0, fmt.Errorf(
		"match type OID for %s or %s: %w", columnType, *dataType, errCannotMatchTypeToVirtualOID,
	)
}

func getTypeClass(
	ctx context.Context,
	columnName string,
	typeName string,
	dataType *string,
) commonmodels.TypeClass {
	defaultTypeClass := commonmodels.TypeClassUnsupported
	typeClass, ok := dbmsdriver.TypeDataNameTypeToClass[typeName]
	if ok {
		return typeClass
	}
	if dataType == nil {
		log.Ctx(ctx).Debug().
			Str("TypeName", typeName).
			Str("ColumnName", columnName).
			Msg("data type is nil, defaulting to unsupported")
		return defaultTypeClass
	}
	typeClass, ok = dbmsdriver.TypeDataNameTypeToClass[*dataType]
	if !ok {
		log.Ctx(ctx).Debug().
			Str("TypeName", typeName).
			Str("DataType", *dataType).
			Str("ColumnName", columnName).
			Msg("cannot match data type to type class, defaulting to unsupported")
		return commonmodels.TypeClassUnsupported
	}
	return typeClass
}

// getColumns - get all columns for a given table
func (i *Introspector) getColumns(ctx context.Context, tx interfaces.DB, tableSchema string, tableName string) ([]mysqlmodels.Column, error) {
	query := `
		select c.COLUMN_NAME,
			   c.COLUMN_TYPE,
			   c.DATA_TYPE,
			   c.NUMERIC_PRECISION,
			   c.NUMERIC_SCALE,
			   c.DATETIME_PRECISION,
			   NOT c.IS_NULLABLE AS is_not_null
		from information_schema.tables t
				 join information_schema.columns c on t.TABLE_NAME = c.TABLE_NAME and t.TABLE_SCHEMA = c.TABLE_SCHEMA
		WHERE t.TABLE_SCHEMA = ? 
		  and t.TABLE_NAME = ?
		ORDER BY c.ORDINAL_POSITION;
	`
	rows, err := tx.QueryContext(ctx, query, tableSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("execute column introspection query: %w", err)
	}
	defer func() {
		if err = rows.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error closing rows")
		}
	}()

	var columns []mysqlmodels.Column
	idx := 0
	for rows.Next() {
		var (
			columnName, columnType         string
			dataType                       *string
			numericPrecision, numericScale *int
			datetimePrecision              *int
			notNull                        bool
		)
		if err := rows.Scan(
			&columnName, &columnType, &dataType, &numericPrecision,
			&numericScale, &datetimePrecision, &notNull,
		); err != nil {
			return nil, fmt.Errorf("scan column introspection row: %w", err)
		}
		typeOID, err := getTypeOID(columnType, dataType)
		if err != nil {
			return nil, fmt.Errorf("get type oid: %w", err)
		}
		typeClass := getTypeClass(ctx, columnName, columnType, dataType)
		columns = append(columns, mysqlmodels.NewColumn(
			idx, columnName, columnType, dataType, numericPrecision,
			numericScale, datetimePrecision, notNull, typeOID, typeClass,
		))
		idx++
	}
	return columns, nil
}

// getPrimaryKey - get primary key columns for a given table.
func (i *Introspector) getPrimaryKey(ctx context.Context, tx interfaces.DB, tableSchema string, tableName string) ([]string, error) {
	query := `
		SELECT k.column_name
		FROM information_schema.table_constraints t
				 JOIN information_schema.key_column_usage k
					  USING (constraint_name, table_schema, table_name)
		WHERE t.constraint_type = 'PRIMARY KEY'
		  AND t.table_schema = ?
		  AND t.table_name = ?
		ORDER BY k.ordinal_position;
	`
	var columns []string
	rows, err := tx.QueryContext(ctx, query, tableSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("execute primary key query: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error closing rows")
		}
	}()
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("scan primary key row: %w", err)
		}
		columns = append(columns, columnName)
	}
	return columns, nil
}

// getForeignKeys - get foreign keys for a given table.
func (i *Introspector) getForeignKeys(
	ctx context.Context,
	tx interfaces.DB,
	tableSchema string,
	tableName string,
) ([]commonmodels.Reference, error) {
	constraints, err := i.getForeignKeyConstraints(ctx, tx, tableSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("get foreign key constraints: %w", err)
	}

	for idx, c := range constraints {
		keys, err := i.getForeignKeyKeys(ctx, tx, c.ConstraintSchema, c.ConstraintName)
		if err != nil {
			return nil, fmt.Errorf("get foreign key keys: %w", err)
		}
		constraints[idx].SetKeys(keys)
	}
	return constraints, nil
}

// getForeignKeyKeys - get foreign key constraint keys for a given constraint.
func (i *Introspector) getForeignKeyKeys(ctx context.Context, tx interfaces.DB, constraintSchema, constraintName string) ([]string, error) {
	query := `
		SELECT k.column_name
		FROM information_schema.key_column_usage k
		WHERE k.CONSTRAINT_SCHEMA = ?
		  AND k.CONSTRAINT_NAME = ?
		ORDER BY k.ordinal_position;
	`
	rows, err := tx.QueryContext(ctx, query, constraintSchema, constraintName)
	if err != nil {
		return nil, fmt.Errorf("execute foreign key keys query: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error closing rows")
		}
	}()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("scan foreign key keys row: %w", err)
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf(
			"get fk column for constraint %s.%s: %w",
			constraintSchema, constraintName, errNoKeysFound,
		)
	}
	return keys, nil
}

// getForeignKeyConstraints - get foreign keys for a given table.
func (i *Introspector) getForeignKeyConstraints(
	ctx context.Context,
	tx interfaces.DB,
	tableSchema string,
	tableName string,
) ([]commonmodels.Reference, error) {
	query := `
		SELECT DISTINCT 
			t.CONSTRAINT_SCHEMA,
			t.CONSTRAINT_NAME,
			k.REFERENCED_TABLE_SCHEMA AS referenced_schema,
			k.REFERENCED_TABLE_NAME AS referenced_table,
			EXISTS (
				SELECT 1
				FROM information_schema.key_column_usage k2
						 JOIN information_schema.COLUMNS c
							  ON k2.COLUMN_NAME = c.COLUMN_NAME
								  AND k2.TABLE_NAME = c.TABLE_NAME
								  AND k2.TABLE_SCHEMA = c.TABLE_SCHEMA
				WHERE k2.CONSTRAINT_SCHEMA = t.CONSTRAINT_SCHEMA
				  AND k2.CONSTRAINT_NAME = t.CONSTRAINT_NAME
				  AND c.IS_NULLABLE = 'YES'
			) AS is_nullable
		FROM information_schema.table_constraints t
				 JOIN information_schema.key_column_usage k
					  ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
						  AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
						  AND t.TABLE_NAME = k.TABLE_NAME
		WHERE t.CONSTRAINT_TYPE = 'FOREIGN KEY'
		  AND t.TABLE_SCHEMA = ?
		  AND t.TABLE_NAME = ?;
	`
	rows, err := tx.QueryContext(ctx, query, tableSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("execute referenced tables query: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error closing rows")
		}
	}()

	var constraints []commonmodels.Reference
	for rows.Next() {
		var (
			constraintSchema, constantName    string
			referencedSchema, referencedTable string
			isNullable                        bool
		)
		if err := rows.Scan(
			&constraintSchema, &constantName, &referencedSchema, &referencedTable, &isNullable,
		); err != nil {
			return nil, fmt.Errorf("scan referenced tables row: %w", err)
		}
		c := commonmodels.NewReference(
			referencedSchema,
			referencedTable,
			constraintSchema,
			constantName,
			nil,
			isNullable,
		)
		constraints = append(constraints, c)
	}
	return constraints, nil
}

func isSystemSchema(s string) bool {
	return s == "information_schema" || s == "performance_schema" || s == "sys"
}
