package transformers

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestNoiseDateTransformer_Transform(t *testing.T) {
	dsn := os.Getenv("GF_TEST_DSN")
	require.NotEmpty(t, dsn, "GF_TEST_DSN env variable must be set")
	c, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()
	loc := time.Now().Location()

	tests := []struct {
		name   string
		column domains.ColumnMeta
		params map[string]interface{}
		input  string
		result struct {
			min, max time.Time
		}
		pattern string
	}{
		{
			name: "test date type",
			column: domains.ColumnMeta{
				TypeName: "date",
				TypeOid:  pgtype.DateOID,
			},
			params: map[string]interface{}{
				"ratio": "1 year 1 mons 1 day 01:01:01.01",
			},
			input: "2023-06-25",
			result: struct{ min, max time.Time }{
				min: time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max: time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
			pattern: `^\d{4}-\d{2}-\d{2}$`,
		},
		{
			name: "test timestamp without timezone type",
			column: domains.ColumnMeta{
				TypeName: "timestamp",
				TypeOid:  pgtype.TimestampOID,
			},
			params: map[string]interface{}{
				"ratio": "1 year 1 mons 1 day 01:01:01.01",
			},
			input: "2023-06-25 00:00:00",
			result: struct{ min, max time.Time }{
				min: time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max: time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
		},
		{
			name: "test timestamp with timezone type",
			column: domains.ColumnMeta{
				TypeName: "timestamptz",
				TypeOid:  pgtype.TimestamptzOID,
			},
			params: map[string]interface{}{
				"ratio": "1 year 1 mons 1 day 01:01:01.01",
			},
			input: "2023-06-25 00:00:00.0+03",
			result: struct{ min, max time.Time }{
				min: time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max: time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}Z$`,
		},
		{
			name: "test timestamp type with Truncate till day",
			column: domains.ColumnMeta{
				TypeName: "timestamp",
				TypeOid:  pgtype.TimestampOID,
			},
			params: map[string]interface{}{
				"ratio":    "1 year 1 mons 1 day 01:01:01.01",
				"truncate": "month",
			},
			input: "2023-06-25 00:00:00",
			result: struct{ min, max time.Time }{
				min: time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max: time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
			pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resTime time.Time
			transformer, err := NewNoiseDateTransformer(tt.column, typeMap, "", tt.params)
			ndt := transformer.(*NoiseDateTransformer)
			require.NoError(t, err)
			res, err := transformer.Transform(tt.input)
			require.NoError(t, err)
			log.Println(res)
			require.Regexp(t, tt.pattern, res)
			err = ndt.Scan(res, &resTime)
			require.NoError(t, err)
			assert.WithinRangef(t, resTime, tt.result.min, tt.result.max, "result is not in range")
		})
	}
}
