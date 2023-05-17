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
		pattern string
	}{
		{
			name: "test date type",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			params: map[string]string{
				"start": "2017-09-14",
				"end":   "2023-09-14",
			},
			pattern: `^\d{4}-\d{2}-\d{2}$`,
		},
		{
			name: "test timestamp without timezone type",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.TimestampOID,
			},
			params: map[string]string{
				"start": "2018-12-15 23:34:17.946707",
				"end":   "2023-09-14 00:00:17.946707",
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
		},
		{
			name: "test timestamp with timezone type",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.TimestamptzOID,
			},
			params: map[string]string{
				"start": "2018-12-15 23:34:17.946707+03",
				"end":   "2023-09-14 00:00:17.946707+03",
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}Z$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NewRandomDateTransformer(tt.column, typeMap, tt.params)
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
			name: "Check start key not existing error",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params:  map[string]string{},
			err:     errors.New("expected start key"),
		},
		{
			name: "Check end key existing error",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]string{
				"start": "2017-09-14",
				//"end":   "2023-09-14",
			},
			err: errors.New("expected end key"),
		},
		{
			name: "Check start key empty value",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]string{
				"start": "",
				//"end":   "2023-09-14",
			},
			err: errors.New("start key cannot be empty string"),
		},
		{
			name: "Check end empty value",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]string{
				"start": "2017-09-14",
				"end":   "",
			},
			err: errors.New("end key cannot be empty string"),
		},
		{
			name: "Invalid start date format",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]string{
				"start": "2017-09-xx",
				"end":   "2017-09-15",
			},
			err: errors.New("cannot decode start value"),
		},
		{
			name: "Invalid end date format",
			column: domains.ColumnMeta{
				Type:    "date",
				TypeOid: pgtype.DateOID,
			},
			typeMap: typeMap,
			params: map[string]string{
				"start": "2017-09-15",
				"end":   "2017-09-xx",
			},
			err: errors.New("cannot decode end value"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRandomDateTransformer(tt.column, tt.typeMap, tt.params)
			require.ErrorContains(t, err, tt.err.Error())

		})
	}
}
