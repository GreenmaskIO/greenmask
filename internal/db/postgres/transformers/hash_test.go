package transformers

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

func getDriver() *toolkit.Driver {
	typeMap := pgtype.NewMap()
	table := &toolkit.Table{
		Schema: "public",
		Name:   "test",
		Oid:    1224,
		Columns: []*toolkit.Column{
			{
				Name:     "id",
				TypeName: "int2",
				TypeOid:  pgtype.Int2OID,
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "created_at",
				TypeName: "timestamp",
				TypeOid:  pgtype.TimestampOID,
				Num:      2,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "title",
				TypeName: "text",
				TypeOid:  pgtype.TextOID,
				Num:      3,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "doc",
				TypeName: "jsonb",
				TypeOid:  pgtype.JSONBOID,
				Num:      4,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "uid",
				TypeName: "uuid",
				TypeOid:  pgtype.UUIDOID,
				Num:      5,
				NotNull:  true,
				Length:   -1,
			},
		},
		Constraints: []toolkit.Constraint{},
	}

	driver, err := toolkit.NewDriver(typeMap, table)
	if err != nil {
		panic(err.Error())
	}
	return driver
}

func TestHashTransformer_Transform(t *testing.T) {
	driver := getDriver()

	transformer, warnings, err := HashTransformerDefinition.Instance(
		context.Background(),
		driver, map[string][]byte{
			"column": []byte("title"),
			"salt":   []byte("12345678"),
		},
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, warnings)

	originRawRecord := []string{"1", toolkit.DefaultNullSeq, "old_value"}

	r, err := transformer.Transform(
		context.Background(),
		toolkit.NewRecord(
			driver,
			originRawRecord,
		),
	)
	require.NoError(t, err)
	transformedRawRecord, err := r.Encode()
	require.NoError(t, err)

	require.Equal(t, transformedRawRecord[2], "9n+v7qGp0ua+DgXtC9ClyjPHjWvWin6fKAmX5bZjcX4=")

}
