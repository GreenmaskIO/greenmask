package transformers

import (
	"log"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/toclib"
)

func TestRandomDateTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	tests := []struct {
		name    string
		table   *toclib.Table
		params  map[string]interface{}
		pattern string
	}{
		{
			name: "test date type",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.DateOID,
					},
				},
			},
			params: map[string]interface{}{
				"min":    "2017-09-14",
				"max":    "2023-09-14",
				"column": "test",
			},
			pattern: `^\d{4}-\d{2}-\d{2}$`,
		},
		{
			name: "test timestamp without timezone type",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.TimestampOID,
					},
				},
			},
			params: map[string]interface{}{
				"min":    "2018-12-15 23:34:17.946707",
				"max":    "2023-09-14 00:00:17.946707",
				"column": "test",
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
		},
		{
			name: "test timestamp with timezone type",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.TimestamptzOID,
					},
				},
			},
			params: map[string]interface{}{
				"min":    "2018-12-15 23:34:17.946707+03",
				"max":    "2023-09-14 00:00:17.946707+03",
				"column": "test",
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}Z$`,
		},
		{
			name: "test timestamp type with Truncate till day",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.TimestampOID,
					},
				},
			},
			params: map[string]interface{}{
				"min":      "2018-12-15 23:34:17.946707",
				"max":      "2023-09-14 00:00:17.946707",
				"truncate": "month",
				"column":   "test",
			},
			pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := RandomDateTransformerMeta.InstanceTransformer(tt.table, typeMap, tt.params)
			require.NoError(t, err)
			tr := transformer.(*RandomDateTransformer)
			val, err := tr.TransformAttr("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}

func TestRandomDateTransformer_Transform_errors(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	tests := []struct {
		name        string
		table       *toclib.Table
		params      map[string]interface{}
		typeMap     *pgtype.Map
		errContains string
	}{
		{
			name: "Check nil typeMap error",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.DateOID,
					},
				},
			},
			typeMap: nil,
			params: map[string]interface{}{
				"column": "test",
			},
			errContains: "typeMap cannot be nil",
		},
		{
			name: "Check min key not existing error",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.DateOID,
					},
				},
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"max":    "2017-09-14",
				"column": "test",
			},
			errContains: "expected Min key",
		},
		{
			name: "Check max key existing error",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.DateOID,
					},
				},
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"min":    "2017-09-14",
				"column": "test",
				//"max":   "2023-09-14",
			},
			errContains: "expected Max key",
		},
		{
			name: "Check min key empty value",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.DateOID,
					},
				},
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"min":    "",
				"column": "test",
				//"max":   "2023-09-14",
			},
			errContains: "expected Min key",
		},
		{
			name: "Check max empty value",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.DateOID,
					},
				},
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"min":    "2017-09-14",
				"max":    "",
				"column": "test",
			},
			errContains: "expected Max key",
		},
		{
			name: "Invalid min date format",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.DateOID,
					},
				},
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"min":    "2017-09-xx",
				"max":    "2017-09-15",
				"column": "test",
			},
			errContains: "cannot decode min value",
		},
		{
			name: "Invalid max date format",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.DateOID,
					},
				},
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"min":    "2017-09-15",
				"max":    "2017-09-xx",
				"column": "test",
			},
			errContains: "cannot decode max value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := RandomDateTransformerMeta.InstanceTransformer(tt.table, tt.typeMap, tt.params)
			require.ErrorContains(t, err, tt.errContains)

		})
	}
}
