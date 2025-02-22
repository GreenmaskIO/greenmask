// Copyright 2023 Greenmask
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

package context

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	trueCond  = "TRUE"
	falseCond = "FALSE"
)

// TODO: Rewrite it using gotemplate

func getDumpObjects(
	ctx context.Context, version int, tx pgx.Tx, options *pgdump.Options,
) ([]*entries.Table, []*entries.Sequence, *entries.Blobs, error) {

	tables, sequesnces, err := getTables(ctx, version, tx, options)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to collect Tables: %w", err)
	}

	// Gather large objects
	lo, err := getLargeObjectsEntries(ctx, tx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to collect large objects: %w", err)
	}

	return tables, sequesnces, lo, nil
}

func getTables(
	ctx context.Context, version int, tx pgx.Tx, options *pgdump.Options,
) ([]*entries.Table, []*entries.Sequence, error) {
	// Building relation search query using regexp adaptation rules and pre-defined query templates
	// TODO: Refactor it to gotemplate
	query, err := buildTableSearchQuery(options.Table, options.ExcludeTable,
		options.ExcludeTableData, options.IncludeForeignData, options.Schema,
		options.ExcludeSchema)
	if err != nil {
		return nil, nil, err
	}

	tableSearchRows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("perform query: %w", err)
	}
	defer tableSearchRows.Close()

	// Generate table objects
	//sequences := make([]*dump_objects.Sequence, 0)
	//Tables := make([]*dump_objects.Table, 0)
	var tables []*entries.Table
	var sequences []*entries.Sequence
	defer tableSearchRows.Close()
	for tableSearchRows.Next() {
		var oid toc.Oid
		var lastVal, relSize int64
		var schemaName, name, owner, rootPtName, rootPtSchema string
		var relKind rune
		var excludeData, isCalled bool

		err = tableSearchRows.Scan(&oid, &schemaName, &name, &owner, &relSize, &relKind,
			&rootPtSchema, &rootPtName, &excludeData, &isCalled, &lastVal,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("scan data: %w", err)
		}

		if excludeData {
			log.Debug().
				Str("TableSchema", name).
				Str("TableName", schemaName).
				Msg("object data excluded")
			continue
		}

		var table *entries.Table
		switch relKind {
		case 'S': // S - sequence
			// Building sequence objects
			s := &entries.Sequence{
				Name:      name,
				Schema:    schemaName,
				Oid:       oid,
				Owner:     owner,
				LastValue: lastVal,
				IsCalled:  isCalled,
			}
			sequences = append(sequences, s)
		case 'r': // r - ordinary table
			fallthrough
		case 'p': // p - partitioned table
			fallthrough
		case 'f': // f - foreign table
			// Building table objects
			table = &entries.Table{
				Table: &toolkit.Table{
					Name:   name,
					Schema: schemaName,
					Oid:    toolkit.Oid(oid),
					Size:   relSize,
				},
				Owner:   owner,
				RelKind: relKind,
				//RootPtSchema:         rootPtSchema,
				//RootPtName:           rootPtName,
				LoadViaPartitionRoot: options.LoadViaPartitionRoot,
			}
			tables = append(tables, table)
		default:
			return nil, nil, fmt.Errorf("unknown relkind \"%c\"", relKind)
		}
	}

	// Assigning columns, pk and fk for each table
	for _, t := range tables {
		if len(t.Columns) > 0 {
			// Columns were already initialized during the transformer initialization
			continue
		}
		columns, err := getColumnsConfig(ctx, tx, t.Oid, version, true)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to collect table columns: %w", err)
		}
		t.Columns = columns

		pkColumns, err := getPrimaryKeyColumns(ctx, tx, t.Oid)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to collect primary key columns: %w", err)
		}
		t.PrimaryKey = pkColumns
	}

	return tables, sequences, nil
}

func getPrimaryKeyColumns(ctx context.Context, tx pgx.Tx, tableOid toolkit.Oid) ([]string, error) {
	row := tx.QueryRow(ctx, PrimaryKeyColumnsQuery, tableOid)
	var columns []string
	if err := row.Scan(&columns); err != nil {
		return nil, fmt.Errorf("error scanning PrimaryKeyColumnsQuery: %w", err)
	}
	return columns, nil
}

func getLargeObjectsEntries(ctx context.Context, tx pgx.Tx) (*entries.Blobs, error) {
	// Collecting large objects metadata
	// Getting large objects table oid

	var tableOid toc.Oid
	row := tx.QueryRow(ctx, LargeObjectsTableOidQuery)
	err := row.Scan(&tableOid)
	if err != nil {
		return nil, fmt.Errorf("error scanning LargeObjectsTableOidQuery: %w", err)
	}

	// Getting list of the all large objects
	var largeObjects []*entries.LargeObject
	loListRows, err := tx.Query(ctx, LargeObjectsListQuery)
	if err != nil {
		return nil, fmt.Errorf("error executing LargeObjectsListQuery: %w", err)
	}
	defer loListRows.Close()
	for loListRows.Next() {
		lo := &entries.LargeObject{TableOid: tableOid}
		if err = loListRows.Scan(&lo.Oid, &lo.Owner, &lo.Comment); err != nil {
			return nil, fmt.Errorf("error scanning LargeObjectsListQuery: %w", err)
		}
		largeObjects = append(largeObjects, lo)
	}

	// Getting list of permission on Large Object
	for _, lo := range largeObjects {

		// Getting default ACL
		defaultACL := &entries.ACL{}
		row = tx.QueryRow(ctx, LargeObjectGetDefaultAclQuery, lo.Oid)
		if err = row.Scan(&defaultACL.Value); err != nil {
			return nil, fmt.Errorf("error scanning LargeObjectGetDefaultAclQuery: %w", err)
		}
		// Getting default ACL items
		var defaultACLItems []*entries.ACLItem
		loDescribeDefaultAclRows, err := tx.Query(ctx, LargeObjectDescribeAclItemQuery, defaultACL.Value)
		if err != nil {
			return nil, fmt.Errorf("error quering LargeObjectDescribeAclItemQuery: %w", err)
		}
		for loDescribeDefaultAclRows.Next() {
			item := &entries.ACLItem{}
			if err = loDescribeDefaultAclRows.Scan(&item.Grantor, &item.Grantee, &item.PrivilegeType, &item.Grantable); err != nil {
				loDescribeDefaultAclRows.Close()
				return nil, fmt.Errorf("error scanning LargeObjectDescribeAclItemQuery: %w", err)
			}
			defaultACLItems = append(defaultACLItems, item)
		}
		loDescribeDefaultAclRows.Close()
		defaultACL.Items = defaultACLItems
		lo.DefaultACL = defaultACL

		// Getting ACL
		var acls []*entries.ACL
		loAclRows, err := tx.Query(ctx, LargeObjectGetAclQuery, lo.Oid)
		if err != nil {
			return nil, fmt.Errorf("error quering LargeObjectGetAclQuery: %w", err)
		}
		loAclRows.Close()
		for loAclRows.Next() {
			a := &entries.ACL{}
			if err = loAclRows.Scan(&a.Value); err != nil {
				loAclRows.Close()
				return nil, fmt.Errorf("error scanning LargeObjectGetAclQuery: %w", err)
			}
			acls = append(acls, a)
		}
		loAclRows.Close()

		// Getting ACL items
		for _, a := range acls {
			var aclItems []*entries.ACLItem
			loDescribeAclRows, err := tx.Query(ctx, LargeObjectDescribeAclItemQuery, a.Value)
			if err != nil {
				return nil, fmt.Errorf("error quering LargeObjectDescribeAclItemQuery: %w", err)
			}
			for loDescribeAclRows.Next() {
				item := &entries.ACLItem{}
				if err = loDescribeAclRows.Scan(&item.Grantor, &item.Grantee, &item.PrivilegeType, &item.Grantable); err != nil {
					loDescribeAclRows.Close()
					return nil, fmt.Errorf("error scanning LargeObjectDescribeAclItemQuery: %w", err)
				}
				aclItems = append(aclItems, item)
			}
			loDescribeAclRows.Close()
			a.Items = aclItems
		}

		lo.ACL = acls
	}

	if len(largeObjects) > 0 {
		return &entries.Blobs{
			LargeObjects: largeObjects,
		}, nil
	}
	return nil, nil
}

func renderRelationCond(ss []string, defaultCond string) (string, error) {
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
		return fmt.Sprintf("(%s)", strings.Join(conds, " OR ")), nil
	}
	return defaultCond, nil
}

func renderNamespaceCond(ss []string, defaultCond string) (string, error) {
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
		return fmt.Sprintf("(%s)", strings.Join(conds, " OR ")), nil
	}
	return defaultCond, nil
}

func renderForeignDataCond(ss []string, defaultCond string) (string, error) {
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
		return fmt.Sprintf("(%s)", strings.Join(conds, " OR ")), nil
	}
	return defaultCond, nil
}

func buildTableSearchQuery(
	includeTable, excludeTable, excludeTableData,
	includeForeignData, includeSchema, excludeSchema []string,
) (string, error) {

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
			   c.oid::TEXT::BIGINT, 
			   n.nspname                              as "Schema",
			   c.relname                              as "Name",
			   pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
			   pg_catalog.pg_relation_size(c.oid) + 
					   coalesce(
						   pg_catalog.pg_relation_size(
							   c.reltoastrelid
						   ), 
						   0
					   ) 							      as "Size",
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
			WHERE c.relkind IN ('p', 'r', 'f', 'S')
			  AND %s     -- relname inclusion
			  AND NOT %s -- relname exclusion
			  AND %s -- schema inclusion
			  AND NOT %s -- schema exclusion
			  AND (s.srvname ISNULL OR %s) -- include foreign data
			  AND n.nspname <> 'pg_catalog'
			  AND n.nspname !~ '^pg_toast'
			  AND n.nspname <> 'information_schema'
			ORDER BY 1
		`

	return fmt.Sprintf(totalQuery, tableDataExclusionCond, tableInclusionCond, tableExclusionCond,
		schemaInclusionCond, schemaExclusionCond, foreignDataInclusionCond), nil
}

func buildSchemaIntrospectionQuery(includeTable, excludeTable, includeForeignData,
	includeSchema, excludeSchema []string,
) (string, error) {

	tableInclusionCond, err := renderRelationCond(includeTable, trueCond)
	if err != nil {
		return "", err
	}
	tableExclusionCond, err := renderRelationCond(excludeTable, falseCond)
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

	totalQuery := `
		SELECT c.oid::TEXT::BIGINT,
			   n.nspname                              as "Schema",
			   c.relname                              as "Name",
			   c.relkind::TEXT                        as "RelKind",
			   (coalesce(pc.oid::BIGINT, 0))             as "RootPtOid",
			   (WITH RECURSIVE part_tables AS (SELECT pg_inherits.inhrelid AS parent_oid,
													  nmsp_child.nspname   AS child_schema,
													  child.oid            AS child_oid,
													  child.relname        AS child,
													  child.relkind        as kind
											   FROM pg_inherits
												   JOIN pg_class child ON pg_inherits.inhrelid = child.oid
												   JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
											   WHERE pg_inherits.inhparent = c.oid
											   UNION
											   SELECT pt.parent_oid,
													  nmsp_child.nspname AS child_schema,
													  child.oid          AS child_oid,
													  child.relname      AS child,
													  child.relkind      as kind
											   FROM part_tables pt
												   JOIN pg_inherits inh ON pt.child_oid = inh.inhparent
												   JOIN pg_class child ON inh.inhrelid = child.oid
												   JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
											   WHERE pt.kind = 'p')
				SELECT array_agg(child_oid::BIGINT) AS oid
				FROM part_tables
				WHERE kind != 'p')                    as "ChildrenPtOids"
		FROM pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_catalog.pg_inherits i ON i.inhrelid = c.oid
			LEFT JOIN pg_catalog.pg_class pc ON i.inhparent = pc.oid AND pc.relkind = 'p'
			LEFT JOIN pg_catalog.pg_namespace pn ON pc.relnamespace = pn.oid
			LEFT JOIN pg_catalog.pg_foreign_table ft ON c.oid = ft.ftrelid
			LEFT JOIN pg_catalog.pg_foreign_server s ON s.oid = ft.ftserver
		WHERE c.relkind IN ('r', 'f', 'p')
          AND %s     -- relname inclusion
          AND NOT %s -- relname exclusion
          AND %s -- schema inclusion
          AND NOT %s -- schema exclusion
          AND (s.srvname ISNULL OR %s) -- include foreign data
		  AND n.nspname <> 'pg_catalog'
		  AND n.nspname !~ '^pg_toast'
		  AND n.nspname <> 'information_schema'
	`

	return fmt.Sprintf(totalQuery, tableInclusionCond, tableExclusionCond,
		schemaInclusionCond, schemaExclusionCond, foreignDataInclusionCond), nil

}
