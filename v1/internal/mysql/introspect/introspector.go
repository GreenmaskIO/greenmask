package introspect

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
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
	tables, err := i.introspectTables(ctx)
	if err != nil {
		return fmt.Errorf("introspect tables: %w", err)
	}

	for _, t := range tables {
		columns, err := i.introspectColumns(ctx, t.Name, t.Schema)
		if err != nil {
			return fmt.Errorf("introspect columns for table %s.%s: %w", t.Schema, t.Name, err)
		}

		t.Columns = columns
		i.tables = append(i.tables, t)
	}

	return nil
}

// introspectTables - get all tables from the database excluding system tables
func (i *Introspector) introspectTables(ctx context.Context) ([]Table, error) {
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
	query, err := template.New("introspectTables").
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

// introspectColumns - get all columns for a given table
func (i *Introspector) introspectColumns(ctx context.Context, tableName, tableSchema string) ([]Column, error) {
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

func getReferences(ctx context.Context, tx pgx.Tx, tableOid toolkit.Oid) ([]*toolkit.Reference, error) {
	var refs []*toolkit.Reference
	rows, err := tx.Query(ctx, foreignKeyColumnsQuery, tableOid)
	if err != nil {
		return nil, fmt.Errorf("error executing ForeignKeyColumnsQuery: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		ref := &toolkit.Reference{}
		if err = rows.Scan(&ref.Schema, &ref.Name, &ref.ReferencedKeys, &ref.IsNullable); err != nil {
			return nil, fmt.Errorf("error scanning ForeignKeyColumnsQuery: %w", err)
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

func (i *Introspector) GetReferencedTables(ctx context.Context, tableName, tableSchema string) ([]Table, error) {
	query := `
		SELECT 
			kcu.TABLE_SCHEMA AS fk_table_schema,
			kcu.TABLE_NAME AS fk_table_name,
			GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) AS curr_table_columns,
			MAX(COLUMNPROPERTY(OBJECT_ID(kcu.TABLE_NAME), kcu.COLUMN_NAME, 'AllowsNull')) AS is_nullable
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
			ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
		WHERE tc.TABLE_NAME = ?
		AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
		GROUP BY kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME;
	`
	rows, err := i.db.QueryContext(ctx, query, tableName, tableSchema)
	if err != nil {
		return nil, fmt.Errorf("execute referenced tables query: %w", err)
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var (
			refTableName, refTableSchema string
		)
		if err := rows.Scan(&refTableName, &refTableSchema); err != nil {
			return nil, fmt.Errorf("scan referenced tables row: %w", err)
		}
		tables = append(tables, NewTable(refTableSchema, refTableName, nil))
	}
	return tables, nil
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
