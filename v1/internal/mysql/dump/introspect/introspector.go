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
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
	mysqlmodels "github.com/greenmaskio/greenmask/v1/internal/mysql/models"
)

var (
	errNoKeysFound                 = fmt.Errorf("no keys found")
	errCannotMatchTypeToVirtualOID = errors.New("cannot match type to virtual OID")
)

type options interface {
	GetIncludedTables() []string
	GetExcludedTables() []string
	GetExcludedSchemas() []string
	GetIncludedSchemas() []string
}

type Introspector struct {
	tables []mysqlmodels.Table
	opt    options
}

func NewIntrospector(opt options) *Introspector {
	return &Introspector{
		opt: opt,
	}
}

func (i *Introspector) GetTables() []mysqlmodels.Table {
	return i.tables
}

func (i *Introspector) GetCommonTables() []commonmodels.Table {
	tables := make([]commonmodels.Table, len(i.tables))
	for idx, table := range i.tables {
		tables[idx] = table.ToCommonTable()
	}
	return tables
}

// Introspect - introspects the mysql instance provided. It received a transaction
// because the data have to be consistent.
// TODO: Keep in ming that mysql does not have schema as in postgresql.
//
//	      It has database and it plays both roles as a schema and database.
//	      Meaning there might be cross references between databases.
//			 Additionally check if possible to open one TX lock snapshot
//			 and import it in the new transaction/session.
func (i *Introspector) Introspect(ctx context.Context, tx *sql.Tx) error {
	tables, err := i.getTables(ctx, tx)
	if err != nil {
		return fmt.Errorf("introspect tables: %w", err)
	}

	for _, t := range tables {
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
func (i *Introspector) getTables(ctx context.Context, tx *sql.Tx) ([]mysqlmodels.Table, error) {
	excludeTables := i.opt.GetExcludedTables()
	excludeSchemas := i.opt.GetExcludedSchemas()
	includeTables := i.opt.GetIncludedTables()
	includeSchemas := i.opt.GetIncludedSchemas()

	data := map[string]interface{}{
		"excludeTables":  excludeTables,
		"excludeSchemas": excludeSchemas,
		"includeTables":  includeTables,
		"includeSchemas": includeSchemas,
	}
	query, err := template.New("getTables").
		Funcs(getFuncMap()).
		Parse(
			`
				select t.TABLE_SCHEMA as schema_name, 
					   t.table_name   as table_name,
					   t.DATA_LENGTH  AS table_size_mb
				from information_schema.tables t
				WHERE
				    TRUE
					AND t.TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
					AND t.TABLE_TYPE = 'BASE TABLE'
				{{- if gt (len .excludeTables) 0 }}
					AND CONCAT(t.TABLE_SCHEMA, '.', t.TABLE_NAME) NOT IN ( {{ len .excludeTables | repeatPlaceholder }} )
				{{- end}}
				{{- if gt (len .includeTables) 0 }}
				    AND CONCAT(t.TABLE_SCHEMA, '.', t.TABLE_NAME) IN ( {{ len .includeTables | repeatPlaceholder  }} )
				{{- end}}
				{{- if gt (len .excludeSchemas) 0 }}
				    AND t.TABLE_SCHEMA NOT IN ( {{ len .excludeSchemas | repeatPlaceholder }} )
				{{- end}}
				{{- if gt (len .includeSchemas) 0 }}
					AND t.TABLE_SCHEMA IN ( {{ len .includeSchemas | repeatPlaceholder  }} )
				{{- end}}
				ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
		`)
	if err != nil {
		return nil, fmt.Errorf("parse introspect tables query template: %w", err)
	}
	buf := new(strings.Builder)
	if err := query.Execute(buf, data); err != nil {
		return nil, fmt.Errorf("execute introspect tables query template: %w", err)
	}
	args := buildArgs(excludeTables, includeTables, excludeSchemas, includeSchemas)
	rows, err := tx.QueryContext(
		ctx,
		buf.String(),
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var tableIDSeq int
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
			ID:     tableIDSeq,
			Name:   tableName,
			Schema: tableSchema,
			Size:   tableSize,
		})
		tableIDSeq++
	}
	return tables, nil
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
func (i *Introspector) getColumns(ctx context.Context, tx *sql.Tx, tableSchema string, tableName string) ([]mysqlmodels.Column, error) {
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
		_ = rows.Close()
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
func (i *Introspector) getPrimaryKey(ctx context.Context, tx *sql.Tx, tableSchema string, tableName string) ([]string, error) {
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
	defer rows.Close()
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
	tx *sql.Tx,
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
func (i *Introspector) getForeignKeyKeys(ctx context.Context, tx *sql.Tx, constraintSchema, constraintName string) ([]string, error) {
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
	defer rows.Close()

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
	tx *sql.Tx,
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
		_ = rows.Close()
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

func buildArgs(args ...interface{}) []interface{} {
	var res []interface{}
	for _, arg := range args {
		switch v := arg.(type) {
		case []string:
			for _, s := range v {
				res = append(res, s)
			}
		default:
			res = append(res, v)
		}
	}
	return res
}

func repeatPlaceholder(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.Repeat("?,", count-1) + "?"
}

func getFuncMap() template.FuncMap {
	fm := sprig.FuncMap()
	fm["repeatPlaceholder"] = repeatPlaceholder
	return fm
}
