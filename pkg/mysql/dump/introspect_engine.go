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

package dump

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	mysqlversion "github.com/greenmaskio/greenmask/pkg/mysql/version"
)

var (
	errIntrospectNoKeysFound    = errors.New("no keys found")
	errIntrospectNoTypeMatch    = errors.New("cannot match type to virtual OID")
	errIntrospectNoSchemasFound = errors.New("no schemas/databases found according to the filters")
)

// introspectSystemSchemas are excluded from introspection by default unless the
// user explicitly includes them.
var introspectSystemSchemas = []string{"information_schema", "performance_schema", "sys", "mysql"}

// schemaScope captures the database/schema-level include/exclude configuration
// used to scope introspection. It deliberately covers schemas/databases ONLY:
// table and data inclusion/exclusion is the ObjectFilter layer's responsibility,
// so introspection never skips tables — it introspects every table in the
// allowed schemas.
type schemaScope struct {
	includeSchemas []*regexp.Regexp
	excludeSchemas []*regexp.Regexp
}

// newSchemaScope compiles the include/exclude schema and database patterns into a
// schemaScope. Database patterns are folded into schema patterns: at the object
// level a MySQL database is a schema.
func newSchemaScope(includeSchemas, excludeSchemas, includeDatabases, excludeDatabases []string) (*schemaScope, error) {
	inc, err := compileSchemaPatterns(append(append([]string{}, includeSchemas...), includeDatabases...))
	if err != nil {
		return nil, fmt.Errorf("compile include schema patterns: %w", err)
	}
	exc, err := compileSchemaPatterns(append(append([]string{}, excludeSchemas...), excludeDatabases...))
	if err != nil {
		return nil, fmt.Errorf("compile exclude schema patterns: %w", err)
	}
	return &schemaScope{includeSchemas: inc, excludeSchemas: exc}, nil
}

// allowed reports whether a schema should be introspected.
func (s *schemaScope) allowed(schema string) bool {
	return s.isIncluded(schema) && !s.isExcluded(schema)
}

func (s *schemaScope) isIncluded(schema string) bool {
	if len(s.includeSchemas) > 0 {
		return matchAnyPattern(s.includeSchemas, schema)
	}
	return true
}

func (s *schemaScope) isExcluded(schema string) bool {
	if matchAnyPattern(s.excludeSchemas, schema) {
		return true
	}
	// System schemas are excluded by default unless explicitly included.
	for _, sys := range introspectSystemSchemas {
		if schema == sys {
			if len(s.includeSchemas) > 0 && matchAnyPattern(s.includeSchemas, schema) {
				return false
			}
			return true
		}
	}
	return false
}

func compileSchemaPatterns(patterns []string) ([]*regexp.Regexp, error) {
	res := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile("^" + p + "$")
		if err != nil {
			return nil, fmt.Errorf("compile regexp %q: %w", p, err)
		}
		res = append(res, re)
	}
	return res, nil
}

func matchAnyPattern(patterns []*regexp.Regexp, s string) bool {
	for _, re := range patterns {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

func repeatPlaceholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.Repeat("?,", count-1) + "?"
}

// introspectEngine performs schema-scoped MySQL introspection. It is a
// self-contained adaptation of the legacy introspector that applies only
// schema/database-level scoping and introspects every table in the allowed
// schemas (no table-level filtering, no NeedDump* flags, no DumpScope).
type introspectEngine struct {
	scope          *schemaScope
	version        core.DBMSVersion
	tables         []mysqlmodels.Table
	allowedSchemas []string
}

func newIntrospectEngine(scope *schemaScope) *introspectEngine {
	return &introspectEngine{scope: scope}
}

// introspect resolves the server version and the allowed schemas, then
// introspects every table in them (columns, primary key, foreign keys).
func (e *introspectEngine) introspect(ctx context.Context, db core.DB) error {
	version, err := e.getVersion(ctx, db)
	if err != nil {
		return fmt.Errorf("get server version: %w", err)
	}
	e.version = version
	log.Ctx(ctx).Debug().
		Str("Version", version.FullString).
		Str("Vendor", version.Vendor()).
		Msg("introspected server version")

	if err := e.getSchemas(ctx, db); err != nil {
		return fmt.Errorf("get schemas: %w", err)
	}

	tables, err := e.getTables(ctx, db)
	if err != nil {
		return fmt.Errorf("introspect tables: %w", err)
	}

	var tableIDSeq int
	for _, t := range tables {
		t.ID = tableIDSeq
		tableIDSeq++

		log.Ctx(ctx).Debug().
			Int("ID", t.ID).
			Str("Schema", t.Schema).
			Str("Table", t.Name).
			Msg("including table in introspection")

		columns, err := e.getColumns(ctx, db, t.Schema, t.Name)
		if err != nil {
			return fmt.Errorf("introspect columns for table %s.%s: %w", t.Schema, t.Name, err)
		}
		t.SetColumns(columns)

		pkColumns, err := e.getPrimaryKey(ctx, db, t.Schema, t.Name)
		if err != nil {
			return fmt.Errorf("introspect primary key for table %s.%s: %w", t.Schema, t.Name, err)
		}
		t.SetPrimaryKey(pkColumns)

		fks, err := e.getForeignKeys(ctx, db, t.Schema, t.Name)
		if err != nil {
			return fmt.Errorf("introspect foreign keys for table %s.%s: %w", t.Schema, t.Name, err)
		}
		t.SetReferences(fks)

		e.tables = append(e.tables, t)
	}

	return nil
}

// getVersion introspects the server version and vendor (MySQL vs MariaDB).
func (e *introspectEngine) getVersion(ctx context.Context, db core.DB) (core.DBMSVersion, error) {
	var versionString, versionComment string
	if err := db.QueryRowContext(ctx, "SELECT VERSION(), @@version_comment").
		Scan(&versionString, &versionComment); err != nil {
		return core.DBMSVersion{}, fmt.Errorf("query server version: %w", err)
	}
	return mysqlversion.ParseServerVersion(versionString, versionComment), nil
}

// getSchemas fetches all non-system schemas and retains those allowed by the
// schema scope. It errors when nothing matches so callers fail fast.
func (e *introspectEngine) getSchemas(ctx context.Context, db core.DB) error {
	query := `
		SELECT SCHEMA_NAME
		FROM information_schema.schemata
		WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'sys')
	`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("execute schemas query: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error closing schemas rows")
		}
	}()

	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return fmt.Errorf("scan schema name: %w", err)
		}
		if e.scope.allowed(schemaName) {
			e.allowedSchemas = append(e.allowedSchemas, schemaName)
		} else {
			log.Ctx(ctx).Debug().
				Str("Schema", schemaName).
				Msg("skipping schema introspection: excluded by schema scope")
		}
	}

	if len(e.allowedSchemas) == 0 {
		return errIntrospectNoSchemasFound
	}
	log.Ctx(ctx).Debug().
		Int("AllowedSchemas", len(e.allowedSchemas)).
		Msg("resolved allowed schemas for introspection")
	return nil
}

// getTables fetches every base table in the allowed schemas. No table-level
// filtering is applied — that is the ObjectFilter layer's responsibility.
func (e *introspectEngine) getTables(ctx context.Context, db core.DB) ([]mysqlmodels.Table, error) {
	if len(e.allowedSchemas) == 0 {
		return nil, nil
	}

	query := `
		SELECT t.TABLE_SCHEMA AS schema_name,
		       t.TABLE_NAME   AS table_name,
		       t.DATA_LENGTH  AS table_size
		FROM information_schema.tables t
		WHERE t.TABLE_TYPE = 'BASE TABLE'
		  AND t.TABLE_SCHEMA IN (` + repeatPlaceholders(len(e.allowedSchemas)) + `)
		ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;`

	args := make([]any, len(e.allowedSchemas))
	for i, s := range e.allowedSchemas {
		args[i] = s
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error closing table rows")
		}
	}()

	var tables []mysqlmodels.Table
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
	}

	log.Ctx(ctx).Debug().
		Int("TotalTables", len(tables)).
		Msg("completed table introspection")
	return tables, nil
}

func introspectTypeOID(columnType string, dataType *string) (core.VirtualOID, error) {
	typeOID, ok := dbmsdriver.TypeNameToVirtualOid[columnType]
	if ok {
		return typeOID, nil
	}
	if dataType == nil {
		return 0, fmt.Errorf("match type OID for %s: %w", columnType, errIntrospectNoTypeMatch)
	}
	typeOID, ok = dbmsdriver.TypeNameToVirtualOid[*dataType]
	if ok {
		return typeOID, nil
	}
	return 0, fmt.Errorf("match type OID for %s or %s: %w", columnType, *dataType, errIntrospectNoTypeMatch)
}

func introspectTypeClass(ctx context.Context, columnName, typeName string, dataType *string) core.TypeClass {
	typeClass, ok := dbmsdriver.TypeDataNameTypeToClass[typeName]
	if ok {
		return typeClass
	}
	if dataType == nil {
		log.Ctx(ctx).Debug().
			Str("TypeName", typeName).
			Str("ColumnName", columnName).
			Msg("data type is nil, defaulting to unsupported")
		return core.TypeClassUnsupported
	}
	typeClass, ok = dbmsdriver.TypeDataNameTypeToClass[*dataType]
	if !ok {
		log.Ctx(ctx).Debug().
			Str("TypeName", typeName).
			Str("DataType", *dataType).
			Str("ColumnName", columnName).
			Msg("cannot match data type to type class, defaulting to unsupported")
		return core.TypeClassUnsupported
	}
	return typeClass
}

func (e *introspectEngine) getColumns(ctx context.Context, db core.DB, tableSchema, tableName string) ([]mysqlmodels.Column, error) {
	query := `
		SELECT c.COLUMN_NAME,
		       c.COLUMN_TYPE,
		       c.DATA_TYPE,
		       c.NUMERIC_PRECISION,
		       c.NUMERIC_SCALE,
		       c.DATETIME_PRECISION,
		       NOT c.IS_NULLABLE AS is_not_null
		FROM information_schema.tables t
		         JOIN information_schema.columns c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
		WHERE t.TABLE_SCHEMA = ?
		  AND t.TABLE_NAME = ?
		ORDER BY c.ORDINAL_POSITION;
	`
	rows, err := db.QueryContext(ctx, query, tableSchema, tableName)
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
		typeOID, err := introspectTypeOID(columnType, dataType)
		if err != nil {
			return nil, fmt.Errorf("get type oid: %w", err)
		}
		typeClass := introspectTypeClass(ctx, columnName, columnType, dataType)
		columns = append(columns, mysqlmodels.NewColumn(
			idx, columnName, columnType, dataType, numericPrecision,
			numericScale, datetimePrecision, notNull, typeOID, typeClass,
		))
		idx++
	}
	return columns, nil
}

func (e *introspectEngine) getPrimaryKey(ctx context.Context, db core.DB, tableSchema, tableName string) ([]string, error) {
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
	rows, err := db.QueryContext(ctx, query, tableSchema, tableName)
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

func (e *introspectEngine) getForeignKeys(ctx context.Context, db core.DB, tableSchema, tableName string) ([]core.Reference, error) {
	constraints, err := e.getForeignKeyConstraints(ctx, db, tableSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("get foreign key constraints: %w", err)
	}

	for idx, c := range constraints {
		keys, err := e.getForeignKeyKeys(ctx, db, c.ConstraintSchema, c.ConstraintName)
		if err != nil {
			return nil, fmt.Errorf("get foreign key keys: %w", err)
		}
		constraints[idx].SetKeys(keys)
	}
	return constraints, nil
}

func (e *introspectEngine) getForeignKeyKeys(ctx context.Context, db core.DB, constraintSchema, constraintName string) ([]string, error) {
	query := `
		SELECT k.column_name
		FROM information_schema.key_column_usage k
		WHERE k.CONSTRAINT_SCHEMA = ?
		  AND k.CONSTRAINT_NAME = ?
		ORDER BY k.ordinal_position;
	`
	rows, err := db.QueryContext(ctx, query, constraintSchema, constraintName)
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
			constraintSchema, constraintName, errIntrospectNoKeysFound,
		)
	}
	return keys, nil
}

func (e *introspectEngine) getForeignKeyConstraints(ctx context.Context, db core.DB, tableSchema, tableName string) ([]core.Reference, error) {
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
	rows, err := db.QueryContext(ctx, query, tableSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("execute referenced tables query: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error closing rows")
		}
	}()

	var constraints []core.Reference
	for rows.Next() {
		var (
			constraintSchema, constraintName  string
			referencedSchema, referencedTable string
			isNullable                        bool
		)
		if err := rows.Scan(
			&constraintSchema, &constraintName, &referencedSchema, &referencedTable, &isNullable,
		); err != nil {
			return nil, fmt.Errorf("scan referenced tables row: %w", err)
		}
		constraints = append(constraints, core.NewReference(
			referencedSchema,
			referencedTable,
			constraintSchema,
			constraintName,
			nil,
			isNullable,
		))
	}
	return constraints, nil
}
