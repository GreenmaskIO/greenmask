package dumpers

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/greenmaskio/greenmask/pkg/toolkit/testutils"
)

func TestTransformationWindow_tryAdd(t *testing.T) {
	ctx := context.Background()
	eg, gtx := errgroup.WithContext(ctx)
	tw := newTransformationWindow(gtx, eg)
	tc := utils.TransformerContext{
		Transformer: &testTransformer{},
	}
	table := getTable()
	require.True(t, tw.tryAdd(table, &tc))
	require.False(t, tw.tryAdd(table, &tc))
}

func TestTransformationWindow_Transform(t *testing.T) {
	mainCtx, mainCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer mainCancel()
	eg, gtx := errgroup.WithContext(mainCtx)
	tw := newTransformationWindow(gtx, eg)
	when, warns := toolkit.NewWhenCond("", nil, nil)
	require.Empty(t, warns)
	tc := utils.TransformerContext{
		Transformer: &testTransformer{},
		When:        when,
	}
	table := getTable()
	require.True(t, tw.tryAdd(table, &tc))

	driver := getDriver(table.Table)
	record := toolkit.NewRecord(driver)
	row := testutils.NewTestRowDriver([]string{"1", "2023-08-27 00:00:00.000000"})
	record.SetRow(row)
	tw.init()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := tw.Transform(ctx, record)
	require.NoError(t, err)
	v, err := record.GetRawColumnValueByName("id")
	require.NoError(t, err)
	require.False(t, v.IsNull)
	require.Equal(t, []byte("2"), v.Data)
	tw.close()
	require.NoError(t, eg.Wait())
}

func TestTransformationWindow_Transform_with_cond(t *testing.T) {
	table := getTable()
	driver := getDriver(table.Table)
	record := toolkit.NewRecord(driver)
	when, warns := toolkit.NewWhenCond("record.id != 1", driver, make(map[string]any))
	require.Empty(t, warns)
	mainCtx, mainCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer mainCancel()
	eg, gtx := errgroup.WithContext(mainCtx)
	tw := newTransformationWindow(gtx, eg)
	tt := &testTransformer{}
	tc := utils.TransformerContext{
		Transformer: tt,
		When:        when,
	}
	require.True(t, tw.tryAdd(table, &tc))

	row := testutils.NewTestRowDriver([]string{"1", "2023-08-27 00:00:00.000000"})
	record.SetRow(row)
	tw.init()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := tw.Transform(ctx, record)
	require.NoError(t, err)
	require.Equal(t, 0, tt.callsCount)
	v, err := record.GetRawColumnValueByName("id")
	require.NoError(t, err)
	require.False(t, v.IsNull)
	require.Equal(t, []byte("1"), v.Data)
	tw.close()
	require.NoError(t, eg.Wait())
}

type testTransformer struct {
	callsCount int
}

func (tt *testTransformer) Init(ctx context.Context) error {
	return nil
}

func (tt *testTransformer) Done(ctx context.Context) error {
	return nil
}

func (tt *testTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	tt.callsCount++
	err := r.SetColumnValueByName("id", 2)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (tt *testTransformer) GetAffectedColumns() map[int]string {
	return map[int]string{
		1: "name",
	}
}

func getDriver(table *toolkit.Table) *toolkit.Driver {
	driver, _, err := toolkit.NewDriver(table, nil)
	if err != nil {
		panic(err.Error())
	}
	return driver
}

func getTable() *entries.Table {
	return &entries.Table{
		Table: &toolkit.Table{
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
		},
	}
}
