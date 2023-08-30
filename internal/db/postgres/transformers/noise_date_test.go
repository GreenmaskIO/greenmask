package transformers

import (
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/toclib"
)

func TestNoiseDateTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)
	loc := time.Now().Location()

	tests := []struct {
		name   string
		table  *toclib.Table
		params map[string]interface{}
		input  string
		result struct {
			min, max time.Time
		}
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
				"ratio":  "1 year 1 mons 1 day 01:01:01.01",
				"column": "test",
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
				"ratio":  "1 year 1 mons 1 day 01:01:01.01",
				"column": "test",
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
				"ratio":  "1 year 1 mons 1 day 01:01:01.01",
				"column": "test",
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
				"ratio":    "1 year 1 mons 1 day 01:01:01.01",
				"truncate": "month",
				"column":   "test",
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
			transformer, err := NoiseDateTransformerMeta.InstanceTransformer(
				tt.table, typeMap, tt.params,
			)
			tr := transformer.(*NoiseDateTransformer)
			require.NoError(t, err)
			res, err := tr.TransformAttr(tt.input)
			require.NoError(t, err)
			log.Println(res)
			require.Regexp(t, tt.pattern, res)
			err = tr.Scan(res, &resTime)
			require.NoError(t, err)
			assert.WithinRangef(t, resTime, tt.result.min, tt.result.max, "result is not in range")
		})
	}
}
