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

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump_objects"
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
	ctx context.Context, tx pgx.Tx, options *pgdump.Options, config map[toolkit.Oid]*dump_objects.Table,
) ([]dump_objects.Entry, error) {

	// Building relation search query using regexp adaptation rules and pre-defined query templates
	// TODO: Refactor it to gotemplate
	query, err := BuildTableSearchQuery(options.Table, options.ExcludeTable,
		options.ExcludeTableData, options.IncludeForeignData, options.Schema,
		options.ExcludeSchema)
	if err != nil {
		return nil, err
	}

	tableSearchRows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("perform query: %w", err)
	}
	defer tableSearchRows.Close()

	// Generate table objects
	//sequences := make([]*dump_objects.Sequence, 0)
	//tables := make([]*dump_objects.Table, 0)
	var dataObjects []dump_objects.Entry
	defer tableSearchRows.Close()
	for tableSearchRows.Next() {
		var oid toc.Oid
		var lastVal int64
		var schemaName, name, owner, rootPtName, rootPtSchema string
		var relKind rune
		var excludeData, isCalled bool
		var ok bool

		err = tableSearchRows.Scan(&oid, &schemaName, &name, &owner, &relKind,
			&rootPtSchema, &rootPtName, &excludeData, &isCalled, &lastVal,
		)
		if err != nil {
			return nil, fmt.Errorf("unnable scan data: %w", err)
		}
		var table *dump_objects.Table

		switch relKind {
		case 'S':
			// Building sequence objects
			dataObjects = append(dataObjects, &dump_objects.Sequence{
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

				table = &dump_objects.Table{
					Table: &toolkit.Table{
						Name:   name,
						Schema: schemaName,
						Oid:    toolkit.Oid(oid),
					},
					Owner:                owner,
					RelKind:              relKind,
					RootPtSchema:         rootPtSchema,
					RootPtName:           rootPtName,
					ExcludeData:          excludeData,
					LoadViaPartitionRoot: options.LoadViaPartitionRoot,
				}
			}

			if table.ExcludeData {
				// TODO: Ensure data exclusion works properly
				continue
			}

			dataObjects = append(dataObjects, table)
		default:
			return nil, fmt.Errorf("unknown relkind \"%c\"", relKind)
		}
	}

	// Assigning columns for each table
	for _, obj := range dataObjects {
		switch v := obj.(type) {
		case *dump_objects.Table:
			columns, err := getColumnsConfig(ctx, tx, v.Oid)
			if err != nil {
				return nil, fmt.Errorf("unable to collect table columns: %w", err)
			}
			v.Columns = columns
		}

	}

	// Collecting large objects metadata
	// Getting large objects table oid
	var tableOid toc.Oid
	row := tx.QueryRow(ctx, LargeObjectsTableOidQuery)
	err = row.Scan(&tableOid)
	if err != nil {
		return nil, fmt.Errorf("error scanning LargeObjectsTableOidQuery: %w", err)
	}

	// Getting list of the all large objects
	var largeObjects []*dump_objects.LargeObject
	loListRows, err := tx.Query(ctx, LargeObjectsListQuery)
	if err != nil {
		return nil, fmt.Errorf("error executing LargeObjectsListQuery: %w", err)
	}
	defer loListRows.Close()
	for loListRows.Next() {
		lo := &dump_objects.LargeObject{TableOid: tableOid}
		if err = loListRows.Scan(&lo.Oid, &lo.Owner, &lo.Comment); err != nil {
			return nil, fmt.Errorf("error scanning LargeObjectsListQuery: %w", err)
		}
		largeObjects = append(largeObjects, lo)
	}

	// Getting list of permission on Large Object
	for _, lo := range largeObjects {

		// Getting default ACL
		defaultACL := &dump_objects.ACL{}
		row = tx.QueryRow(ctx, LargeObjectGetDefaultAclQuery, lo.Oid)
		if err = row.Scan(&defaultACL.Value); err != nil {
			return nil, fmt.Errorf("error scanning LargeObjectGetDefaultAclQuery: %w", err)
		}
		// Getting default ACL items
		var defaultACLItems []*dump_objects.ACLItem
		loDescribeDefaultAclRows, err := tx.Query(ctx, LargeObjectDescribeAclItemQuery, defaultACL.Value)
		if err != nil {
			return nil, fmt.Errorf("error quering LargeObjectDescribeAclItemQuery: %w", err)
		}
		defer loDescribeDefaultAclRows.Close()
		for loDescribeDefaultAclRows.Next() {
			item := &dump_objects.ACLItem{}
			if err = loDescribeDefaultAclRows.Scan(&item.Grantor, &item.Grantee, &item.PrivilegeType, &item.Grantable); err != nil {
				return nil, fmt.Errorf("error scanning LargeObjectDescribeAclItemQuery: %w", err)
			}
			defaultACLItems = append(defaultACLItems, item)
		}
		defaultACL.Items = defaultACLItems
		lo.DefaultACL = defaultACL

		// Getting ACL
		var acls []*dump_objects.ACL
		loAclRows, err := tx.Query(ctx, LargeObjectGetAclQuery, lo.Oid)
		if err != nil {
			return nil, fmt.Errorf("error quering LargeObjectGetAclQuery: %w", err)
		}
		defer loAclRows.Close()
		for loAclRows.Next() {
			a := &dump_objects.ACL{}
			if err = loAclRows.Scan(&a.Value); err != nil {
				return nil, fmt.Errorf("error scanning LargeObjectGetAclQuery: %w", err)
			}
			acls = append(acls, a)
		}

		// Getting ACL items
		for _, a := range acls {
			var aclItems []*dump_objects.ACLItem
			loDescribeAclRows, err := tx.Query(ctx, LargeObjectDescribeAclItemQuery, a.Value)
			if err != nil {
				return nil, fmt.Errorf("error quering LargeObjectDescribeAclItemQuery: %w", err)
			}
			defer loDescribeAclRows.Close()
			for loDescribeAclRows.Next() {
				item := &dump_objects.ACLItem{}
				if err = loDescribeAclRows.Scan(&item.Grantor, &item.Grantee, &item.PrivilegeType, &item.Grantable); err != nil {
					return nil, fmt.Errorf("error scanning LargeObjectDescribeAclItemQuery: %w", err)
				}
				aclItems = append(aclItems, item)
			}
			a.Items = aclItems
		}

		lo.ACL = acls
	}

	if len(largeObjects) > 0 {
		dataObjects = append(dataObjects, &dump_objects.Blobs{
			LargeObjects: largeObjects,
		})
	}

	return dataObjects, nil
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

func BuildTableSearchQuery(
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
