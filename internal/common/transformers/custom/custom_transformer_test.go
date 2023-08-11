package custom

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	domains2 "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/transformers"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

func TestCustomTransformer_Transform(t *testing.T) {
	setting := transformers.NewTransformerSettings().
		SetTransformationType(domains.TupleTransformation).
		SetName("test")

	table := &domains2.TableMeta{
		Oid: 123,
		Columns: []*domains2.Column{
			{
				Name: "test",
				ColumnMeta: domains2.ColumnMeta{
					TypeOid: pgtype.JSONBOID,
				},
			},
		},
	}

	base, err := transformers.NewTransformerBase(table, setting, nil, nil, nil)
	require.NoError(t, err)

	customTransformer := NewCustomTransformer(base, "/usr/bin/cat")

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	tCancel, err := customTransformer.Init(ctx)
	require.NoError(t, err)
	defer tCancel()

	res, err := customTransformer.Transform([]byte("test1\ttest2\n"))
	log.Debug().Str("data", string(res)).Msg("received result")
	require.NoError(t, err)
	require.Equal(t, []byte("test1\ttest2\n"), res)

	res, err = customTransformer.Transform([]byte("test3\ttest4\n"))
	log.Debug().Str("data", string(res)).Msg("received result")
	require.NoError(t, err)
	require.Equal(t, []byte("test3\ttest4\n"), res)

}

func TestCustomTransformer_Validate(t *testing.T) {
	setting := transformers.NewTransformerSettings().
		SetTransformationType(domains.TupleTransformation).
		SetName("test")

	table := &domains2.TableMeta{
		Oid: 123,
		Columns: []*domains2.Column{
			{
				Name: "test",
				ColumnMeta: domains2.ColumnMeta{
					TypeOid: pgtype.JSONBOID,
				},
			},
		},
	}

	base, err := transformers.NewTransformerBase(table, setting, nil, nil, nil)
	require.NoError(t, err)

	customTransformer := NewCustomTransformer(base, "/usr/bin/cat")

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	err = customTransformer.Validate(ctx)
	require.NoError(t, err)
}
