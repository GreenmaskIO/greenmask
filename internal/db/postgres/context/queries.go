package context

var (
	// TableSearchQuery - SQL query for getting table by name and schema
	TableSearchQuery = `	
		SELECT 
		   c.oid::TEXT::INT, 
		   n.nspname                              as "Schema",
		   c.relname                              as "Name",
		   pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
		   c.relkind 							  as "RelKind",
		   (coalesce(pn.nspname, '')) 			  as "rootPtSchema",
		   (coalesce(pc.relname, '')) 			  as "rootPtName",
		   (coalesce(pc.oid, 0))::TEXT::INT       as "rootOid"
        FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_catalog.pg_inherits i ON i.inhrelid = c.oid
                LEFT JOIN  pg_catalog.pg_class pc ON i.inhparent = pc.oid AND pc.relkind = 'p'
            	LEFT JOIN  pg_catalog.pg_namespace pn ON pc.relnamespace = pn.oid
        WHERE c.relkind IN ('r', 'f', 'p')
          AND n.nspname  = $1  -- schema inclusion
          AND c.relname = $2 -- relname inclusion
	`

	// TableColumnsQuery - SQL query for getting all columns of table
	TableColumnsQuery = `
		SELECT 
		    a.attname 										as name,
		    a.atttypid::TEXT::INT                          	as typeoid,
		  	pg_catalog.format_type(a.atttypid, a.atttypmod) as typename,
		  	a.attnotnull 									as notnull,
		  	a.atttypmod 									as mod,
		  	a.attnum 										as num
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	// CustomTypesUsedInTablesQuery - SQL query for listing of all custom types that involved into table definition
	CustomTypesUsedInTablesQuery = `
		WITH used_types AS (
			-- Searching used types in schemas
			SELECT pt.oid AS oid
			FROM pg_catalog.pg_type pt
					 JOIN pg_catalog.pg_namespace pn on pt.typnamespace = pn.oid
			WHERE TRUE
			  -- We assume default types are already defined in the driver and we do not need to
			  -- discover them
			  AND pn.nspname not in ('information_schema', 'pg_catalog')
			  AND exists(SELECT FROM pg_catalog.pg_attribute pa WHERE pa.atttypid = pt.oid)
        ),
	    dependencies AS (
            -- Searching used types and their dependencies (such as base, elemnt or array)
			SELECT oid
			FROM used_types
			UNION
			SELECT pt.oid
			FROM used_types ut
				JOIN pg_catalog.pg_type pt ON ut.oid = pt.typelem OR ut.oid = pt.typarray OR ut.oid = pt.typbasetype
				JOIN pg_catalog.pg_namespace pn on pt.typnamespace = pn.oid
            WHERE pn.nspname not in ('information_schema', 'pg_catalog')
		)
		SELECT pt.oid::TEXT::INT AS oid,
			   pn.nspname     				AS schema,
			   pt.typname     				AS name,
			   pt.typlen      				AS len,
			   pt.typtype     				AS kind,
			   pt.typrelid::TEXT::INT    	AS composed_relation_oid,
			   pt.typelem::TEXT::INT     	AS element_type_oid,
			   pt.typarray::TEXT::INT    	AS array_type_oid,
			   pt.typnotnull  				AS not_null,
			   pt.typbasetype::TEXT::INT 	AS base_type_oid,
			   pc.oid NOTNULL 				AS has_domain_constraint
		FROM dependencies d
				 JOIN pg_catalog.pg_type pt ON d.oid = pt.oid
				 JOIN pg_catalog.pg_namespace pn on pt.typnamespace = pn.oid
				 LEFT JOIN pg_catalog.pg_constraint pc ON pt.oid = pc.contypid;
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
	TablePrimaryKeyReferencesConstraintsQuery = `
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
		WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors($1)
							UNION ALL
							VALUES ($1::regclass))
		  AND contype = 'f'
		  AND conparentid = 0
		ORDER BY conname;
	`

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
)
