package transformers

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	toclib2 "github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/toclib"
)

func getTypeMap() (*pgtype.Map, error) {
	dsn := os.Getenv("GF_TEST_DSN")
	if dsn == "" {
		return nil, errors.New("GF_TEST_DSN env variable must be set")
	}
	c, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		return nil, err
	}
	defer c.Close(context.Background())
	return c.TypeMap(), nil
}

func TestHashTransformer_TransformAttr(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	transformer, err := HashTransformerMeta.InstanceTransformer(
		&toclib2.Table{
			Oid: 123,
			Columns: []*toclib2.Column{
				{
					Name:    "test",
					TypeOid: pgtype.TextOID,
				},
			},
		},
		typeMap,
		map[string]interface{}{
			"column": "test",
			"salt":   "12345678",
		})
	require.NoError(t, err)
	ht := transformer.(*HashTransformer)
	res, err := ht.TransformAttr("old_value")
	require.NoError(t, err)
	require.Equal(t, res, "9n+v7qGp0ua+DgXtC9ClyjPHjWvWin6fKAmX5bZjcX4=")

}
