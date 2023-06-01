package transformers

import (
	"context"
	"log"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestRandomBoolTransformer_Transform(t *testing.T) {
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
			name: "test bool type",
			column: domains.ColumnMeta{
				Type:    "bool",
				TypeOid: pgtype.BoolOID,
			},
			params:  map[string]interface{}{},
			pattern: `^(t|f)$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NewRandomBoolTransformer(tt.column, typeMap, "", tt.params)
			require.NoError(t, err)
			val, err := transformer.Transform("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
