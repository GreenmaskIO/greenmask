package transformers

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestRandomDateTransformer_Transform(t *testing.T) {
	//var connStr = "user=vvoitenko dbname=demo host=/tmp"
	var connStr = "user=postgres dbname=demo"
	c, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()
	// Positive cases
	tests := []struct {
		name    string
		column  domains.ColumnMeta
		params  map[string]interface{}
		pattern string
	}{
		{
			name: "test date type",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			params: map[string]interface{}{
				"min": "2017-09-14",
				"max": "2023-09-14",
			},
			pattern: `^\d{4}-\d{2}-\d{2}$`,
		},
		{
			name: "test timestamp without timezone type",
			column: domains.ColumnMeta{
				Type:    "timestamp",
				TypeOid: pgtype.TimestampOID,
			},
			params: map[string]interface{}{
				"min": "2018-12-15 23:34:17.946707",
				"max": "2023-09-14 00:00:17.946707",
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
		},
		{
			name: "test timestamp with timezone type",
			column: domains.ColumnMeta{
				Type:    "timestamptz",
				TypeOid: pgtype.TimestamptzOID,
			},
			params: map[string]interface{}{
				"min": "2018-12-15 23:34:17.946707+03",
				"max": "2023-09-14 00:00:17.946707+03",
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}Z$`,
		},
		{
			name: "test timestamp type with Truncate till day",
			column: domains.ColumnMeta{
				Type:    "timestamp",
				TypeOid: pgtype.TimestampOID,
			},
			params: map[string]interface{}{
				"min":      "2018-12-15 23:34:17.946707",
				"max":      "2023-09-14 00:00:17.946707",
				"truncate": "month",
			},
			pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NewRandomDateTransformer(tt.column, typeMap, "", tt.params)
			require.NoError(t, err)
			val, err := transformer.Transform("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}

func TestRandomDateTransformer_Transform_errors(t *testing.T) {
	dsn := os.Getenv("GF_TEST_DSN")
	require.NotEmpty(t, dsn, "GF_TEST_DSN env variable must be set")
	c, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()
	// Positive cases
	tests := []struct {
		name        string
		column      domains.ColumnMeta
		params      map[string]interface{}
		typeMap     *pgtype.Map
		errContains string
	}{
		{
			name: "Check nil typeMap error",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap:     nil,
			params:      map[string]interface{}{},
			errContains: "typeMap cannot be nil",
		},
		{
			name: "Check min key not existing error",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"max": "2017-09-14",
			},
			errContains: "expected Min key",
		},
		{
			name: "Check max key existing error",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"min": "2017-09-14",
				//"max":   "2023-09-14",
			},
			errContains: "expected Max key",
		},
		{
			name: "Check min key empty value",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"min": "",
				//"max":   "2023-09-14",
			},
			errContains: "expected Min key",
		},
		{
			name: "Check max empty value",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"min": "2017-09-14",
				"max": "",
			},
			errContains: "expected Max key",
		},
		{
			name: "Invalid min date format",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"min": "2017-09-xx",
				"max": "2017-09-15",
			},
			errContains: "cannot decode min value",
		},
		{
			name: "Invalid max date format",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]interface{}{
				"min": "2017-09-15",
				"max": "2017-09-xx",
			},
			errContains: "cannot decode max value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRandomDateTransformer(tt.column, tt.typeMap, "", tt.params)
			require.ErrorContains(t, err, tt.errContains)

		})
	}
}
