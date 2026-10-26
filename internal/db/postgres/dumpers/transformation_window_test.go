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
	table := getTable("")
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
	table := getTable("")
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
	table := getTable("")
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

func getTable(when string) *entries.Table {
	t := &entries.Table{
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
	d := getDriver(t.Table)
	t.Driver = d
	whenCond, warns := toolkit.NewWhenCond(when, d, make(map[string]any))
	if len(warns) > 0 {
		panic("unable to create when condition")
	}
	t.When = whenCond
	return t
}

type setColumnTransformer struct {
	column     string
	value      any
	callsCount int
}

func (st *setColumnTransformer) Init(ctx context.Context) error { return nil }

func (st *setColumnTransformer) Done(ctx context.Context) error { return nil }

func (st *setColumnTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	st.callsCount++
	return r, r.SetColumnValueByName(st.column, st.value)
}

func (st *setColumnTransformer) GetAffectedColumns() map[int]string {
	return map[int]string{0: st.column}
}

func TestTransformationWindow_tryAdd_when_cond_starts_new_window(t *testing.T) {
	table := getTable("")
	eg, gtx := errgroup.WithContext(context.Background())
	tw := newTransformationWindow(gtx, eg)
	noCond, warns := toolkit.NewWhenCond("", table.Driver, map[string]any{})
	require.Empty(t, warns)
	cond, warns := toolkit.NewWhenCond("record.id == 1", table.Driver, map[string]any{})
	require.Empty(t, warns)

	require.True(t, tw.tryAdd(table, &utils.TransformerContext{
		Transformer: &setColumnTransformer{column: "id", value: 2}, When: noCond,
	}))
	require.False(t, tw.tryAdd(table, &utils.TransformerContext{
		Transformer: &setColumnTransformer{column: "created_at", value: time.Now()}, When: cond,
	}))

	// a transformer with a when condition can open a window
	tw = newTransformationWindow(gtx, eg)
	require.True(t, tw.tryAdd(table, &utils.TransformerContext{
		Transformer: &setColumnTransformer{column: "id", value: 2}, When: cond,
	}))
}

func TestTransformationWindow_tryAdd_dynamic_parameter_conflict(t *testing.T) {
	table := getTable("")
	eg, gtx := errgroup.WithContext(context.Background())
	noCond, warns := toolkit.NewWhenCond("", table.Driver, map[string]any{})
	require.Empty(t, warns)
	writesID := &utils.TransformerContext{Transformer: &setColumnTransformer{column: "id", value: 2}, When: noCond}
	readsID := &utils.TransformerContext{
		Transformer: &setColumnTransformer{column: "created_at", value: time.Now()},
		When:        noCond,
		DynamicParameters: map[string]*toolkit.DynamicParameter{
			"min": {DynamicValue: &toolkit.DynamicParamValue{Column: "id"}},
		},
	}

	tw := newTransformationWindow(gtx, eg)
	require.True(t, tw.tryAdd(table, writesID))
	require.False(t, tw.tryAdd(table, readsID))

	tw = newTransformationWindow(gtx, eg)
	require.True(t, tw.tryAdd(table, readsID))
	require.False(t, tw.tryAdd(table, writesID))
}

// sequential semantics: the second transformer condition must see the first transformer output
func TestTransformationPipeline_TransformAsync_when_cond_sees_previous_output(t *testing.T) {
	table := getTable("")
	noCond, warns := toolkit.NewWhenCond("", table.Driver, map[string]any{})
	require.Empty(t, warns)
	cond, warns := toolkit.NewWhenCond("record.id == 1", table.Driver, map[string]any{})
	require.Empty(t, warns)
	second := &setColumnTransformer{column: "created_at", value: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
	table.TransformersContext = []*utils.TransformerContext{
		{Transformer: &setColumnTransformer{column: "id", value: 2}, When: noCond},
		{Transformer: second, When: cond},
	}

	mainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	eg, gtx := errgroup.WithContext(mainCtx)
	var tws []*transformationWindow
	tw := newTransformationWindow(gtx, eg)
	tws = append(tws, tw)
	for _, tc := range table.TransformersContext {
		if !tw.tryAdd(table, tc) {
			tw = newTransformationWindow(gtx, eg)
			tws = append(tws, tw)
			require.True(t, tw.tryAdd(table, tc))
		}
	}
	require.Len(t, tws, 2)
	tp := &TransformationPipeline{table: table, transformationWindows: tws, isAsync: true}
	for _, w := range tws {
		w.init()
	}

	for i := 0; i < 100; i++ {
		record := toolkit.NewRecord(table.Driver)
		record.SetRow(testutils.NewTestRowDriver([]string{"1", "2023-08-27 00:00:00.000000"}))
		_, err := tp.TransformAsync(context.Background(), record)
		require.NoError(t, err)
	}
	for _, w := range tws {
		w.close()
	}
	require.NoError(t, eg.Wait())
	require.Equal(t, 0, second.callsCount)
}
