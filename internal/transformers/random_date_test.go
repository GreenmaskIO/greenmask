package transformers

import (
	"context"
	"errors"
	"log"
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
			name: "test date generating for text type as default timestamp type",
			column: domains.ColumnMeta{
				Type:    "text",
				TypeOid: pgtype.TextOID,
			},
			params: map[string]interface{}{
				"min": "2018-09-14 00:00:17.0",
				"max": "2023-09-14 00:00:17.0",
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
		},
		{
			name: "test date generating for text type as date type",
			column: domains.ColumnMeta{
				Type:    "text",
				TypeOid: pgtype.TextOID,
			},
			params: map[string]interface{}{
				"min":     "2018-09-14",
				"max":     "2023-09-14",
				"useType": "date",
			},
			pattern: `^\d{4}-\d{2}-\d{2}$`,
		},
		{
			name: "test date generating for text type as timestamp type",
			column: domains.ColumnMeta{
				Type:    "text",
				TypeOid: pgtype.TextOID,
			},
			params: map[string]interface{}{
				"min":     "2018-09-14 00:00:17.0",
				"max":     "2023-09-14 00:00:17.0",
				"useType": "timestamp",
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
		},
		{
			name: "test date generating for text type as timestamptz type",
			column: domains.ColumnMeta{
				Type:    "text",
				TypeOid: pgtype.TextOID,
			},
			params: map[string]interface{}{
				"min":     "2018-12-15 23:34:17.946707+03",
				"max":     "2023-09-14 00:00:17.946707+03",
				"useType": "timestamptz",
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}Z$`,
		},
		{
			name: "test date generating for text type as timestamptz type",
			column: domains.ColumnMeta{
				Type:    "text",
				TypeOid: pgtype.TextOID,
			},
			params: map[string]interface{}{
				"min":     "2018-12-15 23:34:17.946707+03",
				"max":     "2023-09-14 00:00:17.946707+03",
				"useType": "timestamptz",
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
			transformer, err := NewRandomDateTransformerV2(tt.column, typeMap, tt.params)
			require.NoError(t, err)
			val, err := transformer.Transform("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}

func TestRandomDateTransformer_Transform_errors(t *testing.T) {
	var connStr = "user=vvoitenko dbname=demo host=/tmp"
	c, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()
	// Positive cases
	tests := []struct {
		name    string
		column  domains.ColumnMeta
		params  map[string]string
		typeMap *pgtype.Map
		err     error
	}{
		{
			name: "Check nil typeMap error",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: nil,
			params:  map[string]string{},
			err:     errors.New("typeMap cannot be nil"),
		},
		{
			name: "Check min key not existing error",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params:  map[string]string{},
			err:     errors.New("expected min key"),
		},
		{
			name: "Check max key existing error",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]string{
				"min": "2017-09-14",
				//"max":   "2023-09-14",
			},
			err: errors.New("expected max key"),
		},
		{
			name: "Check min key empty value",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]string{
				"min": "",
				//"max":   "2023-09-14",
			},
			err: errors.New("min key cannot be empty string"),
		},
		{
			name: "Check max empty value",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]string{
				"min": "2017-09-14",
				"max": "",
			},
			err: errors.New("max key cannot be empty string"),
		},
		{
			name: "Invalid min date format",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]string{
				"min": "2017-09-xx",
				"max": "2017-09-15",
			},
			err: errors.New("cannot decode min value"),
		},
		{
			name: "Invalid max date format",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]string{
				"min": "2017-09-15",
				"max": "2017-09-xx",
			},
			err: errors.New("cannot decode max value"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRandomDateTransformer(tt.column, tt.typeMap, tt.params)
			require.ErrorContains(t, err, tt.err.Error())

		})
	}
}
