package utils

import (
	"context"
	"testing"
	"time"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestTransformer struct {
	p map[string]*toolkit.Parameter
}

func (tt *TestTransformer) Init(ctx context.Context) error {
	return nil
}

func (tt *TestTransformer) Done(ctx context.Context) error {
	return nil
}

func (tt *TestTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	return r, nil
}

func NewTestTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (
	Transformer, toolkit.ValidationWarnings, error,
) {
	return &TestTransformer{
		p: parameters,
	}, nil, nil
}

func TestDefinition(t *testing.T) {

	TestTransformerDefinition := NewDefinition(
		NewTransformerProperties("test", "simple description"),
		NewTestTransformer,
		toolkit.MustNewParameter("column", "a column name", new(string), nil).
			SetIsColumn(toolkit.NewColumnProperties().
				SetAffected(true).
				SetAllowedColumnTypes("timestamp"),
			),
		toolkit.MustNewParameter("replace", "replacement value", &time.Time{}, nil).
			SetLinkParameter("column"),
	)

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
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
		},
		Constraints: []toolkit.Constraint{},
	}

	driver, err := toolkit.NewDriver(typeMap, table, nil)
	require.NoError(t, err)

	rawParams := map[string]toolkit.ParamsValue{
		"column":  []byte("created_at"),
		"replace": []byte("2023-08-27 12:08:11.304895"),
	}

	_, warnings, err := TestTransformerDefinition.Instance(context.Background(), driver, rawParams, nil)
	require.NoError(t, err)
	assert.Empty(t, warnings)
}
