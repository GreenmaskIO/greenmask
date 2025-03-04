package mysql

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Introspector struct {
	db     *sql.DB
	tables []Table
}

func NewIntrospector(db *sql.DB) Introspector {
	return Introspector{
		db: db,
	}
}

func (i *Introspector) Introspect() error {
	tables, err := i.introspectTables()
	if err != nil {
		return fmt.Errorf("introspect tables: %w", err)
	}

	for _, t := range tables {
		columns, err := i.introspectColumns(t.Name, t.Schema)
		if err != nil {
			return fmt.Errorf("introspect columns for table %s.%s: %w", t.Schema, t.Name, err)
		}

		t.Columns = columns
		i.tables = append(i.tables, t)
	}

	return nil
}

// introspectTables - get all tables from the database excluding system tables
func (i *Introspector) introspectTables() ([]Table, error) {
	query := `
		select t.TABLE_SCHEMA as schema_name, 
		       t.table_name   as table_name,
			   t.DATA_LENGTH  AS table_size_mb
		from information_schema.tables t
		WHERE t.TABLE_SCHEMA not in ('information_schema', 'performance_schema');
	`
	rows, err := i.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []Table
	for rows.Next() {
		var (
			tableName, tableSchema string
			tableSize              int64
		)
		if err := rows.Scan(&tableSchema, &tableName, &tableSize); err != nil {
			return nil, err
		}
		tables = append(tables, NewTable(tableSchema, tableName, tableSize))
	}
	return tables, nil
}

// introspectColumns - get all columns for a given table
func (i *Introspector) introspectColumns(tableName, tableSchema string) ([]Column, error) {
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
	rows, err := i.db.Query(query, tableSchema, tableName)
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
