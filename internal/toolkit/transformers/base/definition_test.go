package base

import (
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
	"testing"
	"time"
)

func TestDefinition(t *testing.T) {
	TestTransformerDefinition := &Definition{
		Properties: MustNewProperties("test", "simple description", TupleTransformation),
		//New:        NewTestTransformer,
		Parameters: []*transformers.Parameter{
			transformers.MustNewParameter("column", "a column name", new(int), nil, nil, nil).
				SetIsColumn(),
			transformers.MustNewParameter("replace", "replacement value", &time.Time{}, nil, nil, nil).
				SetLinkParameter("column"),
		},
	}

	typeMap := pgtype.NewMap()
	table := &transformers.Table{
		Schema: "public",
		Name:   "test",
		Oid:    1224,
		Columns: []*transformers.Column{
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
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
		},
		Constraints: []*transformers.Constraint{},
	}

	driver, err := transformers.NewDriver(typeMap, table)
	require.NoError(t, err)

	rawParams := map[string][]byte{
		"column":  []byte("created_at"),
		"replace": []byte("2023-08-27 12:08:11.304895+03"),
	}

	err = TestTransformerDefinition.ParseParameters(driver, rawParams)
	require.NoError(t, err)

	//TestTransformerDefinition.New(context.Background(), driver, rawParams)
}
