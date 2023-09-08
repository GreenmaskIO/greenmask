package custom

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	domains2 "github.com/greenmaskio/greenmask/internal/db/postgres/lib/domains"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
)

func TestCustomTransformer_Transform(t *testing.T) {
	setting := utils.NewTransformerSettings().
		SetTransformationType(domains.TupleTransformation).
		SetName("test")

	table := &data_objects.TableMeta{
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

	base, err := utils.NewTransformerBase(table, setting, nil, nil, nil)
	require.NoError(t, err)

	customTransformer := NewCustomTransformer(base, "/usr/bin/cat")

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	tCancel, err := customTransformer.InitTransformation(ctx)
	require.NoError(t, err)
	defer tCancel()

	res, err := customTransformer.Transform([]byte("test1\ttest2\n"))
	log.Debug().Str("data", string(res)).Msg("received result")
	require.NoError(t, err)
	require.Equal(t, []byte("test1\ttest2"), res)

	res, err = customTransformer.Transform([]byte("test3\ttest4\n"))
	log.Debug().Str("data", string(res)).Msg("received result")
	require.NoError(t, err)
	require.Equal(t, []byte("test3\ttest4"), res)
}

func TestCustomTransformer_Validate(t *testing.T) {
	setting := utils.NewTransformerSettings().
		SetTransformationType(domains.TupleTransformation).
		SetName("test")

	table := &data_objects.TableMeta{
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

	base, err := utils.NewTransformerBase(table, setting, nil, nil, nil)
	require.NoError(t, err)

	customTransformer, _ := NewCustomTransformer(context.Background(), base, "/usr/bin/bash", "-c", "echo 1")

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	warnings, err := customTransformer.Validate(ctx)
	assert.NoError(t, err)
	assert.Empty(t, warnings)
}
