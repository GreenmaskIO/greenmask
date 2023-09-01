package context

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/dump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/pgdump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/toc"
	toolkit "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
)

const (
	trueCond  = "TRUE"
	falseCond = "FALSE"
)

// TODO: Rewrite it using gotemplate

func getDumpObjects(ctx context.Context, tx pgx.Tx, options *pgdump.Options, config map[toolkit.Oid]*dump.Table) ([]dump.Entry, error) {

	// Building relation search query using regexp adaptation rules and pre-defined query templates
	// TODO: Refactor it to gotemplate
	query, err := BuildTableSearchQuery(options.Table, options.ExcludeTable,
		options.ExcludeTableData, options.IncludeForeignData, options.Schema,
		options.ExcludeSchema)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("perform query: %w", err)
	}

	// Generate table objects
	//sequences := make([]*dump.Sequence, 0)
	//tables := make([]*dump.Table, 0)
	var dataObjects []dump.Entry
	defer rows.Close()
	for rows.Next() {
		var oid toc.Oid
		var lastVal int64
		var schemaName, name, owner, rootPtName, rootPtSchema string
		var relKind rune
		var excludeData, isCalled bool
		var ok bool

		if err = rows.Scan(&oid, &schemaName, &name, &owner, &relKind,
			&rootPtSchema, &rootPtName, &excludeData, &isCalled, &lastVal,
		); err != nil {
			return nil, fmt.Errorf("unnable scan data: %w", err)
		}
		var table *dump.Table

		switch relKind {
		case 'S':
			// Building sequence objects
			dataObjects = append(dataObjects, &dump.Sequence{
				Name:        name,
				Schema:      schemaName,
				Oid:         oid,
				Owner:       owner,
				LastValue:   lastVal,
				IsCalled:    isCalled,
				ExcludeData: excludeData,
			})
		case 'r':
			fallthrough
		case 'p':
			fallthrough
		case 'f':
			// Building table objects
			table, ok = config[toolkit.Oid(oid)]
			if ok {
				// If table was discovered during Transformer validation - use that object instead of a new
				table.ExcludeData = excludeData
				table.LoadViaPartitionRoot = options.LoadViaPartitionRoot
			} else {
				// If table is not found - create new table object and collect all the columns

				columns, err := getColumnsConfig(ctx, tx, toolkit.Oid(oid))
				if err != nil {
					return nil, fmt.Errorf("unable to collect table columns: %w", err)
				}

				table = &dump.Table{
					Table: &toolkit.Table{
						Name:    name,
						Schema:  schemaName,
						Oid:     toolkit.Oid(oid),
						Columns: columns,
					},
					Owner:                owner,
					RelKind:              relKind,
					RootPtSchema:         rootPtSchema,
					RootPtName:           rootPtName,
					ExcludeData:          excludeData,
					LoadViaPartitionRoot: options.LoadViaPartitionRoot,
				}
			}

			dataObjects = append(dataObjects, table)
		default:
			return nil, fmt.Errorf("unknown relkind \"%s\"", relKind)
		}
	}

	return dataObjects, nil
}

func renderRelationCond(ss []string, defaultCond string) (string, error) {
	res := defaultCond
	template := "(n.nspname ~ '%s' AND c.relname ~ '%s')"
	if len(ss) > 0 {
		conds := make([]string, 0, len(ss))
		for _, item := range ss {
			var schemaPattern = ".*"
			var tablePattern string
			regexpParts := strings.Split(item, ".")
			if len(regexpParts) > 2 {
				return "", errors.New("dots must appear only once")
			} else if len(regexpParts) == 2 {
				s, err := pgdump.AdaptRegexp(regexpParts[0])
				if err != nil {
					return "", fmt.Errorf("cannot adapt schema pattern: %w", err)
				}
				schemaPattern = s
				s, err = pgdump.AdaptRegexp(regexpParts[1])
				if err != nil {
					return "", fmt.Errorf("cannot adapt table pattern: %w", err)
				}
				tablePattern = s
			} else {
				s, err := pgdump.AdaptRegexp(regexpParts[0])
				if err != nil {
					return "", fmt.Errorf("cannot adapt table pattern: %w", err)
				}
				tablePattern = s
			}

			conds = append(conds, fmt.Sprintf(template, schemaPattern, tablePattern))
		}
		res = fmt.Sprintf("(%s)", strings.Join(conds, " OR "))
		return res, nil
	}
	return defaultCond, nil
}

func renderNamespaceCond(ss []string, defaultCond string) (string, error) {
	res := defaultCond
	template := "(n.nspname ~ '%s')"
	if len(ss) > 0 {
		conds := make([]string, 0, len(ss))
		for _, item := range ss {
			if len(strings.Split(item, ".")) > 1 {
				return "", errors.New("does not expect dots")
			}

			pattern, err := pgdump.AdaptRegexp(item)
			if err != nil {
				return "", fmt.Errorf("cannot adapt schema pattern: %w", err)
			}

			conds = append(conds, fmt.Sprintf(template, pattern))
		}
		res = fmt.Sprintf("(%s)", strings.Join(conds, " OR "))
		return res, nil
	}
	return defaultCond, nil
}

func renderForeignDataCond(ss []string, defaultCond string) (string, error) {
	res := defaultCond
	template := "(s.srvname ~ '%s')"
	if len(ss) > 0 {
		conds := make([]string, 0, len(ss))
		for _, item := range ss {
			if len(strings.Split(item, ".")) > 1 {
				return "", errors.New("does not expect dots")
			}

			pattern, err := pgdump.AdaptRegexp(item)
			if err != nil {
				return "", fmt.Errorf("cannot adapt schema pattern: %w", err)
			}

			conds = append(conds, fmt.Sprintf(template, pattern))
		}
		res = fmt.Sprintf("(%s)", strings.Join(conds, " OR "))
		return res, nil
	}
	return defaultCond, nil
}

func BuildTableSearchQuery(includeTable, excludeTable, excludeTableData,
	includeForeignData, includeSchema, excludeSchema []string) (string, error) {

	tableInclusionCond, err := renderRelationCond(includeTable, trueCond)
	if err != nil {
		return "", err
	}
	tableExclusionCond, err := renderRelationCond(excludeTable, falseCond)
	if err != nil {
		return "", err
	}
	tableDataExclusionCond, err := renderRelationCond(excludeTableData, falseCond)
	if err != nil {
		return "", err
	}
	schemaInclusionCond, err := renderNamespaceCond(includeSchema, trueCond)
	if err != nil {
		return "", err
	}
	schemaExclusionCond, err := renderNamespaceCond(excludeSchema, falseCond)
	if err != nil {
		return "", err
	}

	foreignDataInclusionCond, err := renderForeignDataCond(includeForeignData, falseCond)
	if err != nil {
		return "", err
	}

	// --         WHERE c.relkind IN ('r', 'p', '') (array['r', 'S', 'v', 'm', 'f', 'p'])
	totalQuery := `
		SELECT 
		   c.oid::TEXT::INT, 
		   n.nspname                              as "Schema",
		   c.relname                              as "Name",
		   pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
		   c.relkind 							  as "RelKind",
		   (coalesce(pn.nspname, '')) 			  as "rootPtSchema",
		   (coalesce(pc.relname, '')) 			  as "rootPtName",
		   (%s) 							      as "ExcludeData", -- data exclusion
		   CASE 
		       WHEN c.relkind = 'S' THEN
				   CASE 
					   WHEN  pg_sequence_last_value(c.oid::regclass) ISNULL THEN
						   FALSE
					   ELSE
						   TRUE
				   END
			   ELSE 
		       	 FALSE
		  	END AS "IsCalled",
			CASE 
			    WHEN c.relkind = 'S' THEN 
			    	coalesce(pg_sequence_last_value(c.oid::regclass), sq.seqstart)
				ELSE 
					0
			END	  AS "LastVal"
        FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_catalog.pg_inherits i ON i.inhrelid = c.oid
                LEFT JOIN  pg_catalog.pg_class pc ON i.inhparent = pc.oid AND pc.relkind = 'p'
            	LEFT JOIN  pg_catalog.pg_namespace pn ON pc.relnamespace = pn.oid
            	LEFT JOIN pg_catalog.pg_foreign_table ft ON c.oid = ft.ftrelid
            	LEFT JOIN pg_catalog.pg_foreign_server s ON s.oid = ft.ftserver
				LEFT JOIN pg_catalog.pg_sequence sq ON c.oid = sq.seqrelid
        WHERE c.relkind IN ('r', 'f', 'S')
          AND %s     -- relname inclusion
          AND NOT %s -- relname exclusion
          AND %s -- schema inclusion
          AND NOT %s -- schema exclusion
          AND (s.srvname ISNULL OR %s) -- include foreign data
		  AND n.nspname <> 'pg_catalog'
		  AND n.nspname !~ '^pg_toast'
		  AND n.nspname <> 'information_schema'
	`

	return fmt.Sprintf(totalQuery, tableDataExclusionCond, tableInclusionCond, tableExclusionCond,
		schemaInclusionCond, schemaExclusionCond, foreignDataInclusionCond), nil
}
