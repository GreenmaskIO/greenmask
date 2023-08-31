package dump_config_builder

var (
	TableSearchQuery = `	
		SELECT 
		   c.oid::TEXT::INT, 
		   n.nspname                              as "Schema",
		   c.relname                              as "Name",
		   pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
		   c.relkind 							  as "RelKind",
		   (coalesce(pn.nspname, '')) 			  as "rootPtSchema",
		   (coalesce(pc.relname, '')) 			  as "rootPtName",
		   (coalesce(pc.oid, 0)) 			      as "rootOid"
        FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_catalog.pg_inherits i ON i.inhrelid = c.oid
                LEFT JOIN  pg_catalog.pg_class pc ON i.inhparent = pc.oid AND pc.relkind = 'p'
            	LEFT JOIN  pg_catalog.pg_namespace pn ON pc.relnamespace = pn.oid
        WHERE c.relkind IN ('r', 'f', 'p')
          AND n.nspname  = $1  -- schema inclusion
          AND c.relname = $2 -- relname inclusion
	`
	TableColumnsQuery = `
		SELECT 
		    a.attname,
		    a.atttypid 	as typeoid,
		  	pg_catalog.format_type(a.atttypid, a.atttypmod) as typename,
		  	a.attnotnull,
		  	a.atttypmod,
		  	a.attnum
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`
	TableConstraintsCommonQuery = `
		SELECT pc.conname                                    AS "name",
			   pn.nspname                                    AS "schema",
			   pc.contype                                    AS "type",
			   pc.conparentid::TEXT::INT                     AS root_pt_constraint_oid,
			   pc.confrelid::TEXT::INT                       AS fk_ref_oid,
			   pc.conkey                                     AS constrained_column_oids,
			   pc.confkey                                    AS constrained_column_fk_oids,
			   pg_catalog.pg_get_constraintdef(pc.oid, true) AS def
		FROM pg_constraint pc
				 JOIN pg_namespace pn on pc.connamespace = pn.oid
		WHERE pc.conrelid = $1
	`
	TableDomainConstraintsQuery = `
		SELECT t.oid,
			   n.nspname                                          as "schema",
			   t.typname                                          as "name",
			   t.typbasetype,
			   t.typtypmod,
			   pg_catalog.format_type(t.typbasetype, t.typtypmod) as "typeName",
			   t.typnotnull                                       as "nullable",
			   pg_catalog.array_to_string(ARRAY(
												  SELECT pg_catalog.pg_get_constraintdef(r.oid, true)
												  FROM pg_catalog.pg_constraint r
												  WHERE t.oid = r.contypid
											  ), ' ')             as "check"
		FROM pg_catalog.pg_type t
				 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
				 LEFT JOIN pg_catalog.pg_description d ON d.classoid = t.tableoid AND d.objoid = t.oid AND d.objsubid = 0
		WHERE t.typtype = 'd'
		  AND n.nspname <> 'pg_catalog'
		  AND n.nspname <> 'information_schema'
		  AND pg_catalog.pg_type_is_visible(t.oid)
		--   AND
		ORDER BY 1, 2;
	`
	TablePrimaryKeyReferencesConstraintsQuery = `
		SELECT conname                                      AS "name",
			   pn.nspname                                   AS "schema",
			   pc.contype                                   AS "type",
			   NULL                                         AS domain_oid,
			   pc.conparentid::TEXT::INT                    AS root_pt_constraint_oid,
			   pc.confrelid::TEXT::INT                      AS fk_ref_oid,
			   pc.conkey                                    AS constrained_column_oids,
			   pc.confkey                                   AS constrained_column_fk_oids,
		
			   conrelid::pg_catalog.regclass                AS ontable,
			   pg_catalog.pg_get_constraintdef(c.oid, true) AS condef
		FROM pg_catalog.pg_constraint pc
				 JOIN pg_catalog.pg_namespace pn on pc.connamespace = pn.oid
				 JOIN pg_catalog.pg_class c ON pc.conrelid = c.oid
				 JOIN pg_catalog.pg_namespace cn ON c.relnamespace = cn.oid
		WHERE confrelid IN (SELECT pg_catalog.pg_partition_ancestors('24999')
							UNION ALL
							VALUES ('24999'::regclass))
		  AND contype = 'f'
		  AND conparentid = 0
		ORDER BY conname;
	`
)
