package dump_config_builder

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/regexp_adapter"
)

const (
	trueCond  = "TRUE"
	falseCond = "FALSE"
)

// TODO: Rewrite it using gotemplate

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
				s, err := regexp_adapter.AdaptRegexp(regexpParts[0])
				if err != nil {
					return "", fmt.Errorf("cannot adapt schema pattern: %w", err)
				}
				schemaPattern = s
				s, err = regexp_adapter.AdaptRegexp(regexpParts[1])
				if err != nil {
					return "", fmt.Errorf("cannot adapt table pattern: %w", err)
				}
				tablePattern = s
			} else {
				s, err := regexp_adapter.AdaptRegexp(regexpParts[0])
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

			pattern, err := regexp_adapter.AdaptRegexp(item)
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

			pattern, err := regexp_adapter.AdaptRegexp(item)
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
