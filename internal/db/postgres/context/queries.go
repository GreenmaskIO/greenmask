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

import "text/template"

var (
	tableExistsQuery = `
		SELECT exists(
			SELECT 1
			FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relkind IN ('r', 'f', 'p')
			  AND n.nspname  = $1  -- schema inclusion
			  AND c.relname = $2 -- relname inclusion
        );
	`

	// TableSearchQuery - SQL query for getting table by name and schema
	TableSearchQuery = `	
		SELECT 
		   c.oid::TEXT::INT, 
		   n.nspname                              as "Schema",
		   c.relname                              as "Name",
		   pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
		   c.relkind 							  as "RelKind",
		   (coalesce(pn.nspname, '')) 			  as "RootPtSchema",
		   (coalesce(pc.relname, '')) 			  as "RootPtName"
-- 		   (coalesce(pc.oid, 0))::TEXT::INT       as "RootOid"
        FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_catalog.pg_inherits i ON i.inhrelid = c.oid
                LEFT JOIN  pg_catalog.pg_class pc ON i.inhparent = pc.oid AND pc.relkind = 'p'
            	LEFT JOIN  pg_catalog.pg_namespace pn ON pc.relnamespace = pn.oid
        WHERE c.relkind IN ('r', 'f', 'p')
          AND n.nspname  = $1  -- schema inclusion
          AND c.relname = $2 -- relname inclusion
	`

	TableGetChildPatsQuery = `
		WITH RECURSIVE part_tables AS (SELECT pg_inherits.inhrelid AS parent_oid,
											  nmsp_child.nspname   AS child_schema,
											  child.oid            AS child_oid,
											  child.relname        AS child,
											  child.relkind        as kind
									   FROM pg_inherits
												JOIN pg_class child ON pg_inherits.inhrelid = child.oid
												JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
									   WHERE pg_inherits.inhparent = $1
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
		SELECT child_oid::INT  AS oid
		FROM part_tables
		WHERE kind != 'p';
    `

	// TableColumnsQuery - SQL query for getting all columns of table
	TableColumnsQuery = template.Must(template.New("TableColumnsQuery").Parse(`
		SELECT 
		    a.attname 										as name,
		    a.atttypid::TEXT::INT                          	as typeoid,
		  	pg_catalog.format_type(a.atttypid, a.atttypmod) as typename,
		  	a.attnotnull 									as notnull,
		  	a.atttypmod 									as att_len,
		  	a.attnum 										as num,
		  	t.typlen 										as type_len
			{{ if ge .Version 120000 }}
		  	,a.attgenerated != ''	    				    as attgenerated
			{{ end }}
		FROM pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`))

	CustomTypesWithTypeChainQuery = `
		with RECURSIVE
			custom_types AS (
				-- Collecting all used custom types
				select pt.oid,
					   pt.typbasetype,
					   1 as num
				from pg_type pt
						 JOIN pg_catalog.pg_namespace pn on pt.typnamespace = pn.oid
				WHERE TRUE
		--       AND pt.typtype in ('b', 'd', 'e', 'r')
				  AND pn.nspname NOT IN ('pg_catalog', 'information_schema')
				  AND exists(SELECT FROM pg_catalog.pg_attribute pa WHERE pa.atttypid = pt.oid)
				UNION
				-- trying to find the whole types inheritance chain
				SELECT ct.oid,
					   pt.typbasetype,
					   num + 2 as num
				FROM custom_types ct
						 JOIN pg_type pt ON ct.typbasetype = pt.oid),
			basexbase AS (SELECT oid, typbasetype, num
						  FROM custom_types
						  UNION
						  SELECT ct.typbasetype AS oid, pt.typbasetype, num
						  FROM custom_types ct
						  JOIN pg_type pt ON ct.typbasetype = pt.oid
            ),
			types_with_chain AS (
               SELECT ct.oid, coalesce (array_agg(ct.typbasetype ORDER BY num) FILTER ( WHERE ct.typbasetype != 0 ), ARRAY []::INT []) AS chain
			   FROM basexbase ct
			   GROUP BY ct.oid
			)
			SELECT pt.oid::TEXT::INT         AS oid,
				   twc.chain::INT[]          AS chain_oids,
                   (select array_agg(t.typname)::TEXT[] FROM pg_type t WHERE t.oid = ANY (twc.chain)) AS chain_names,
				   pn.nspname                AS schema,
				   pt.typname                AS name,
				   pt.typlen                 AS len,
				   pt.typtype                AS kind,
				   pt.typrelid::TEXT::INT    AS composed_relation_oid,
				   pt.typelem::TEXT::INT     AS element_type_oid,
				   pt.typarray::TEXT::INT    AS array_type_oid,
				   pt.typnotnull             AS not_null,
				   pt.typbasetype::TEXT::INT AS base_type_oid,
				   pc.oid::TEXT::INT NOTNULL AS has_domain_constraint
			FROM types_with_chain twc
					 JOIN pg_catalog.pg_type pt ON twc.oid = pt.oid
					 JOIN pg_catalog.pg_namespace pn on pt.typnamespace = pn.oid
					 LEFT JOIN pg_catalog.pg_constraint pc ON pt.oid = pc.contypid
			ORDER BY pt.oid; -- Sorting according to oid number it helps to register correctly by creation order
	`

	// TableConstraintsCommonQuery - SQL query for searching the common constraints (pk, fk, trigger, check, exclude)
	// Purpose - find all constraints assigned to the discovering table (excluding Domain constraints)
	TableConstraintsCommonQuery = `
		SELECT pc.oid::TEXT::INT,                                            -- constraint oid
			   pc.conname                                    AS "name",     -- constraint name
			   pn.nspname                                    AS "schema",   -- constraint schema name
			   pc.contype                                    AS "type",     -- constraint type
			   pc.conkey                                     AS columns,    -- constrained columns at the source table (from which search is performing)
			   coalesce(rt.oid::TEXT::INT, 0)                AS rt_oid,     -- referenced table oid
			   coalesce(rtn.nspname, '')                     AS rt_name,    -- referenced table name
			   coalesce(rt.relname, '')                      AS rt_schema,  -- referenced table schema
			   pc.confkey                                    AS rt_columns, -- referenced table involved columns into constraint
			   pg_catalog.pg_get_constraintdef(pc.oid, true) AS def         -- textual constraint definition
		FROM pg_constraint pc
				 JOIN pg_namespace pn on pc.connamespace = pn.oid
				 LEFT JOIN pg_class rt ON pc.confrelid = rt.oid -- referenced table (rt)
				 LEFT JOIN pg_namespace rtn ON rt.relnamespace = rtn.oid
		WHERE conrelid = $1;
	`

	// TablePrimaryKeyReferencesConstraintsQuery - SQL query for collecting all the PK references
	TablePrimaryKeyReferencesConstraintsQuery = template.Must(
		template.New("TablePrimaryKeyReferencesConstraintsQuery").Parse(`
		SELECT pc.oid::TEXT::INT,
			   pn.nspname                                    AS "schema",
			   pc.conname                                    AS "name",
			   c.oid::TEXT::INT                              AS on_table_oid,
			   cn.nspname                                    AS on_table_schema,
			   c.relname                                     AS on_table_name,
			   pc.conkey                                     AS on_table_constrained_columns,
			   pg_catalog.pg_get_constraintdef(pc.oid, true) AS condef
		FROM pg_catalog.pg_constraint pc
				 JOIN pg_catalog.pg_namespace pn on pc.connamespace = pn.oid
				 JOIN pg_catalog.pg_class c ON pc.confrelid = c.oid
				 JOIN pg_catalog.pg_namespace cn ON c.relnamespace = cn.oid
	{{ if ge .Version 120000 }}
		WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors($1)
							UNION ALL
							VALUES ($1::regclass))
	{{ else }}
		WHERE confrelid = $1::regclass
	{{ end }}
		  AND contype = 'f'
		  AND conparentid = 0
		ORDER BY conname;
	`))

	// DomainConstraintsQuery - SQL query for getting domain check constraint
	DomainConstraintsQuery = `
		SELECT pc.oid::TEXT::INT,                                         -- constraint oid
			   pn.nspname                                    AS "schema", -- constraint schema name
			   pc.conname                                    AS "name",   -- constraint name
			   pg_catalog.pg_get_constraintdef(pc.oid, true) AS def       -- textual constraint definition
		FROM pg_constraint pc
				 JOIN pg_namespace pn on pc.connamespace = pn.oid
				 LEFT JOIN pg_class rt ON pc.confrelid = rt.oid
				 LEFT JOIN pg_namespace rtn ON rt.relnamespace = rtn.oid
		WHERE contypid = $1 AND contype = 'c';
	`

	LargeObjectsTableOidQuery = `
		SELECT 
		    pc.oid::INT
		FROM pg_catalog.pg_class pc
			JOIN pg_catalog.pg_namespace pn ON pc.relnamespace = pn.oid
		WHERE pc.relname = 'pg_largeobject' AND pn.nspname = 'pg_catalog'
	`

	LargeObjectsListQuery = `
		SELECT 
		  oid::INT,
		  pg_catalog.pg_get_userbyid(lomowner) as "owner",
		  coalesce(pg_catalog.obj_description(oid, 'pg_largeobject'), '') as "comment"
		FROM pg_catalog.pg_largeobject_metadata
		ORDER BY oid
	`

	LargeObjectGetDefaultAclQuery = `
		SELECT 
		  dacl.acl::TEXT
		FROM pg_catalog.pg_largeobject_metadata plm, 
		     unnest(acldefault('L', plm.lomowner)) dacl(acl)
		WHERE plm.oid = $1
		LIMIT 1
	`

	LargeObjectGetAclQuery = `
		SELECT 
		  acl.acl::TEXT
		FROM pg_catalog.pg_largeobject_metadata plm, 
		     unnest(lomacl) acl(acl)
		WHERE plm.oid = $1
	`

	LargeObjectDescribeAclItemQuery = `
		SELECT 
		  grantor_role.rolname  AS grantor,
		  greantee_role.rolname AS grantee,
		  acl.privilege_type,
		  acl.is_grantable
		FROM aclexplode(ARRAY[$1]::ACLITEM[]) acl
			 JOIN pg_catalog.pg_roles grantor_role ON grantor_role.oid = acl.grantor
			 JOIN pg_catalog.pg_roles greantee_role ON greantee_role.oid = acl.grantee
	`

	PrimaryKeyColumnsQuery = `
		select array_agg(DISTINCT a.attname) AS pk_columns
		from pg_catalog.pg_constraint pcp
			JOIN pg_catalog.pg_attribute a ON a.attrelid = pcp.conrelid AND a.attnum = ANY (pcp.conkey) AND pcp.contype = 'p'
		WHERE pcp.conrelid = $1;
	`
)
