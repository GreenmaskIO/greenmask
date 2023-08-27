package transformers

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefinition(t *testing.T) {
	TestTransformerDefinition := &Definition{
		Properties: MustNewProperties("test", "simple description", TupleTransformation),
		//New:        NewTestTransformer,
		Parameters: []*Parameter{
			MustNewParameter("column", "a column name", new(int), nil, nil, nil).
				SetIsColumn(&ColumnProperties{
					Affected:           true,
					AllowedColumnTypes: []string{"timestamp"},
				}),
			MustNewParameter("replace", "replacement value", &time.Time{}, nil, nil, nil).
				SetLinkParameter("column"),
		},
	}

	typeMap := pgtype.NewMap()
	table := &Table{
		Schema: "public",
		Name:   "test",
		Oid:    1224,
		Columns: []*Column{
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
		Constraints: []Constraint{},
	}

	driver, err := NewDriver(typeMap, table)
	require.NoError(t, err)

	rawParams := map[string][]byte{
		"column":  []byte("created_at"),
		"replace": []byte("2023-08-27 12:08:11.304895+03"),
	}

	transformer, warmings, err := TestTransformerDefinition.Instance(context.Background(), driver, rawParams)
	require.NoError(t, err)
	assert.Empty(t, warmings)
	rec, err := transformer.Transform(context.Background(), NewRecord(driver, []string{"test"}))
	require.NoError(t, err)
	assert.Equal(t, rec, NewRecord(driver, []string{"test"}))
}
