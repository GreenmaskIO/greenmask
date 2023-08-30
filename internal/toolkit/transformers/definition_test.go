package transformers

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestTransformer struct {
	p map[string]*Parameter
}

func (tt *TestTransformer) Init(ctx context.Context) error {
	return nil
}

func (tt *TestTransformer) Validate(ctx context.Context) (ValidationWarnings, error) {
	return nil, nil
}

func (tt *TestTransformer) Transform(ctx context.Context, r *Record) (*Record, error) {
	return r, nil
}

func NewTestTransformer(ctx context.Context, driver *Driver, parameters map[string]*Parameter) (Transformer, error) {
	return &TestTransformer{
		p: parameters,
	}, nil
}

func TestDefinition(t *testing.T) {

	TestTransformerDefinition := NewDefinition(
		MustNewProperties("test", "simple description", TupleTransformation),
		NewTestTransformer,
		[]*Parameter{
			MustNewParameter("column", "a column name", new(string), nil).
				SetIsColumn(NewColumnProperties().
					SetAffected(true).
					SetAllowedColumnTypes("timestamp"),
				),
			MustNewParameter("replace", "replacement value", &time.Time{}, nil).
				SetLinkParameter("column"),
		},
	)

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
		"replace": []byte("2023-08-27 12:08:11.304895"),
	}

	transformer, warnings, err := TestTransformerDefinition.Instance(context.Background(), driver, rawParams)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	rec, err := transformer.Transform(context.Background(), NewRecord(driver, []string{"test"}))
	require.NoError(t, err)
	assert.Equal(t, rec, NewRecord(driver, []string{"test"}))
}
