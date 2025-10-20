package transformers

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestRandomNumericTransformer_Transform(t *testing.T) {
	tests := []struct {
		name             string
		staticParameters map[string]commonmodels.ParamsValue
		dynamicParameter map[string]commonmodels.DynamicParamValue
		original         []*commonmodels.ColumnRawValue
		validateFn       func(t *testing.T, recorder commonininterfaces.Recorder)
		expectedErr      string
		columns          []commonmodels.Column
		isNull           bool
	}{
		{
			name: "numeric",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("1234567"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column": commonmodels.ParamsValue("data"),
				"engine": commonmodels.ParamsValue("deterministic"),
				"min":    commonmodels.ParamsValue("10000000000000000000"),
				"max":    commonmodels.ParamsValue("100000000000000000000"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				expectedMin := decimal.RequireFromString("10000000000000000000")
				expectedMax := decimal.RequireFromString("100000000000000000000")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
			},
		},
		{
			name: "keep_null false and NULL seq",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("deterministic"),
				"min":       commonmodels.ParamsValue("10000000000000000000"),
				"max":       commonmodels.ParamsValue("100000000000000000000"),
				"keep_null": commonmodels.ParamsValue("false"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue(nil, true)},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				expectedMin := decimal.RequireFromString("10000000000000000000")
				expectedMax := decimal.RequireFromString("100000000000000000000")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
			},
		},
		{
			name: "keep_null false and NULL seq",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("deterministic"),
				"min":       commonmodels.ParamsValue("10000000000000000000"),
				"max":       commonmodels.ParamsValue("100000000000000000000"),
				"keep_null": commonmodels.ParamsValue("true"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue(nil, true)},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.True(t, isNull)
			},
		},
		{
			name: "Implicitly set threshold",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("deterministic"),
				"min":       commonmodels.ParamsValue("0.0"),
				"max":       commonmodels.ParamsValue("10.0"),
				"keep_null": commonmodels.ParamsValue("false"),
				"decimal":   commonmodels.ParamsValue("2"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue(nil, true)},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				expectedMin := decimal.RequireFromString("0.0")
				expectedMax := decimal.RequireFromString("10.0")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
			},
		},
		{
			name: "Dynamic mode",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("deterministic"),
				"keep_null": commonmodels.ParamsValue("false"),
				"decimal":   commonmodels.ParamsValue("2"),
			},
			dynamicParameter: map[string]commonmodels.DynamicParamValue{
				"min": {
					Column: "min_val",
				},
				"max": {
					Column: "max_val",
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("1234"), false),
				commonmodels.NewColumnRawValue([]byte("-1000020102102"), false),
				commonmodels.NewColumnRawValue([]byte("10"), false),
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
				{
					Idx:      1,
					Name:     "min_val",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
				{
					Idx:      2,
					Name:     "max_val",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				expectedMin := decimal.RequireFromString("-1000020102102")
				expectedMax := decimal.RequireFromString("10")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				RandomNumericTransformerDefinition,
				tt.columns,
				tt.staticParameters,
				tt.dynamicParameter,
			)
			err := env.InitParameters(t, ctx)
			require.NoError(t, commonutils.PrintValidationWarnings(ctx, vc, nil, true))
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			err = env.InitTransformer(t, ctx)
			require.NoError(t, commonutils.PrintValidationWarnings(ctx, vc, nil, true))
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			env.SetRecord(t, tt.original...)

			err = env.Transform(t, ctx)
			require.NoError(t, commonutils.PrintValidationWarnings(ctx, vc, nil, true))
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}
			tt.validateFn(t, env.GetRecord())
		})
	}
}
