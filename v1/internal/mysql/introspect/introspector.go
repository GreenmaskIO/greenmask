package introspect

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	_ "github.com/go-sql-driver/mysql"

	"github.com/greenmaskio/greenmask/v1/internal/common/domains"
)

var (
	errNoKeysFound = fmt.Errorf("no keys found")
)

type options interface {
	GetIncludedTables() []string
	GetExcludedTables() []string
	GetExcludedSchemas() []string
	GetIncludedSchemas() []string
}

type Introspector struct {
	db     *sql.DB
	tables []Table
	opt    options
}

func NewIntrospector(db *sql.DB, opt options) Introspector {
	return Introspector{
		db:  db,
		opt: opt,
	}
}

func (i *Introspector) Introspect(ctx context.Context) error {
	tables, err := i.getTables(ctx)
	if err != nil {
		return fmt.Errorf("introspect tables: %w", err)
	}

	for _, t := range tables {
		columns, err := i.getColumns(ctx, t.Schema, t.Name)
		if err != nil {
			return fmt.Errorf("introspect columns for table %s.%s: %w", t.Schema, t.Name, err)
		}
		t.SetColumns(columns)

		pkColumns, err := i.getPrimaryKey(ctx, t.Schema, t.Name)
		if err != nil {
			return fmt.Errorf("introspect primary key for table %s.%s: %w", t.Schema, t.Name, err)
		}
		t.SetPrimaryKey(pkColumns)

		fks, err := i.getForeignKeys(ctx, t.Name, t.Schema)
		if err != nil {
			return fmt.Errorf("introspect foreign keys for table %s.%s: %w", t.Schema, t.Name, err)
		}
		t.SetReferences(fks)

		i.tables = append(i.tables, t)
	}

	return nil
}

// getTables - get all tables from the database excluding system tables
func (i *Introspector) getTables(ctx context.Context) ([]Table, error) {
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
					t.TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
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
	println(buf.String())
	args := buildArgs(excludeTables, includeTables, excludeSchemas, includeSchemas)
	rows, err := i.db.QueryContext(
		ctx,
		buf.String(),
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []Table
	for rows.Next() {
		var (
			tableName, tableSchema string
			tableSize              *int64
		)
		if err := rows.Scan(&tableSchema, &tableName, &tableSize); err != nil {
			return nil, err
		}
		tables = append(tables, NewTable(tableSchema, tableName, tableSize))
	}
	return tables, nil
}

// getColumns - get all columns for a given table
func (i *Introspector) getColumns(ctx context.Context, tableSchema string, tableName string) ([]Column, error) {
	query := `
		select c.COLUMN_NAME,
			   c.COLUMN_TYPE,
			   c.DATA_TYPE,
			   c.NUMERIC_PRECISION,
			   c.NUMERIC_SCALE,
			   c.DATETIME_PRECISION,
			   NOT c.IS_NULLABLE AS is_not_null
		from information_schema.tables t
				 join information_schema.columns c on t.TABLE_NAME = c.TABLE_NAME
		WHERE t.TABLE_SCHEMA = ? 
		  and t.TABLE_NAME = ?
		ORDER BY c.ORDINAL_POSITION;
	`
	rows, err := i.db.QueryContext(ctx, query, tableSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("execute column introspection query: %w", err)
	}
	defer rows.Close()

	var columns []Column
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
		columns = append(columns, NewColumn(
			idx, columnName, columnType, dataType, numericPrecision,
			numericScale, datetimePrecision, notNull,
		))
		idx++
	}
	return columns, nil
}

// getPrimaryKey - get primary key columns for a given table.
func (i *Introspector) getPrimaryKey(ctx context.Context, tableSchema string, tableName string) ([]string, error) {
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
	rows, err := i.db.QueryContext(ctx, query, tableSchema, tableName)
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
	tableSchema string,
	tableName string,
) ([]domains.Reference, error) {
	constraints, err := i.getForeignKeyConstraints(ctx, tableSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("get foreign key constraints: %w", err)
	}

	for idx, c := range constraints {
		keys, err := i.getForeignKeyKeys(ctx, c.ConstraintSchema, c.ConstraintName)
		if err != nil {
			return nil, fmt.Errorf("get foreign key keys: %w", err)
		}
		constraints[idx].SetKeys(keys)
	}
	return constraints, nil
}

// getForeignKeyKeys - get foreign key constraint keys for a given constraint.
func (i *Introspector) getForeignKeyKeys(ctx context.Context, constraintSchema, constraintName string) ([]string, error) {
	query := `
		SELECT k.column_name
		FROM information_schema.key_column_usage k
		WHERE k.CONSTRAINT_SCHEMA = ?
		  AND k.CONSTRAINT_NAME = ?
		ORDER BY k.ordinal_position;
	`
	rows, err := i.db.QueryContext(ctx, query, constraintSchema, constraintName)
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
	tableSchema string,
	tableName string,
) ([]domains.Reference, error) {
	query := `
		SELECT t.CONSTRAINT_SCHEMA,
			   t.CONSTRAINT_NAME,
			   exists(select 1
					  from information_schema.key_column_usage k
							   JOIN information_schema.COLUMNS c
									ON k.COLUMN_NAME = c.COLUMN_NAME AND k.TABLE_NAME = c.TABLE_NAME AND
									   k.TABLE_SCHEMA = c.TABLE_SCHEMA
					  where k.CONSTRAINT_SCHEMA = t.CONSTRAINT_SCHEMA
						AND k.CONSTRAINT_NAME = t.CONSTRAINT_NAME
						AND c.IS_NULLABLE = 'YES') as is_nullable
		FROM information_schema.table_constraints t
		WHERE t.constraint_type = 'FOREIGN KEY'
		  AND t.table_schema = ?
		  AND t.table_name = ?;
	`
	rows, err := i.db.QueryContext(ctx, query, tableSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("execute referenced tables query: %w", err)
	}
	defer rows.Close()

	var constraints []domains.Reference
	for rows.Next() {
		var (
			constraintSchema, constantName string
			isNullable                     bool
		)
		if err := rows.Scan(&constraintSchema, &constantName, &isNullable); err != nil {
			return nil, fmt.Errorf("scan referenced tables row: %w", err)
		}
		c := domains.NewReference(
			tableSchema,
			tableName,
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
