
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
			   (((n.nspname ~ '^(bookings)$' AND c.relname ~ '^(flights)$'))) 							      as "ExcludeData", -- data exclusion
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
				  END 									AS "IsCalled",
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
			  AND ((n.nspname ~ '^(bookings)$' AND c.relname ~ '^(.*)$'))     -- relname inclusion
			  AND NOT ((n.nspname ~ '^(booki.*)$' AND c.relname ~ '^(boarding_pas.*)$') OR (n.nspname ~ '^(b..*)$' AND c.relname ~ '^(seats)$')) -- relname exclusion
			  AND ((n.nspname ~ '^(booki.*)$')) -- schema inclusion
			  AND NOT ((n.nspname ~ '^(public.*[[:digit:]].*1)$')) -- schema exclusion
			  AND (s.srvname ISNULL OR ((s.srvname ~ '^(myserver)$'))) -- include foreign data
			  AND n.nspname <> 'pg_catalog'
			  AND n.nspname !~ '^pg_toast'
			  AND n.nspname <> 'information_schema'
			ORDER BY 1
		