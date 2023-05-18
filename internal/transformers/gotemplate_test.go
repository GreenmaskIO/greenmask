package transformers

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestGoTemplateTransformer_Transform(t *testing.T) {
	var connStr = "user=vvoitenko dbname=demo host=/tmp"
	c, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()

	var tmpl = `{{ if eq . "test" }}res1{{ else }}res2{{ end }}`
	transformer, err := NewGoTemplateTransformer(domains.ColumnMeta{
		Type:    "date",
		TypeOid: pgtype.DateOID,
	}, typeMap, map[string]string{"template": tmpl})
	require.NoError(t, err)
	res, err := transformer.Transform("test")
	require.NoError(t, err)
	assert.Equal(t, "res1", res)
}
