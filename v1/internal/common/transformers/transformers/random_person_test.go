package transformers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators/transformers"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func dumpColumnContainers(columnContainers ...any) []byte {
	res, err := json.Marshal(columnContainers)
	if err != nil {
		panic(err)
	}
	return res
}

func assertStringContainsOneOfItemFromList(t *testing.T, val string, values []string) {
	t.Helper()
	for _, item := range values {
		if strings.Contains(val, item) {
			return
		}
	}
	require.Failf(t, "value does not contain any of the expected items", "value: %s, expected items: %v", val, values)
}

func TestRandomPersonTransformer_Transform(t *testing.T) {
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
					TypeName: mysqldbmsdriver.TypeText,
					TypeOID:  mysqldbmsdriver.VirtualOidText,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("1234567"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"columns": dumpColumnContainers(
					randomPersonColumns{
						Name:     "data",
						Template: "{{ .Title }} {{ .FirstName }} {{ .LastName }}",
						Hashing:  true,
						HashOnly: false,
					},
				),
				"engine": commonmodels.ParamsValue("deterministic"),
				"gender": commonmodels.ParamsValue("Any"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				rawVal, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, rawVal.IsNull)
				log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultFirstNamesFemale)
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultFirstNamesMale)
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultLastNames)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				RandomPersonTransformerDefinition,
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

//func TestRandomPersonTransformer_Transform_static_firstname(t *testing.T) {
//
//	columnName := "data"
//	originalValue := "John Dust123"
//	params := map[string]toolkit.ParamsValue{
//		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .FirstName }}"}]`),
//		"engine":  toolkit.ParamsValue("random"),
//		"gender":  toolkit.ParamsValue("Any"),
//	}
//
//	driver, record := getDriverAndRecord(columnName, originalValue)
//	def, ok := utils.DefaultTransformerRegistry.Get("RandomPerson")
//	require.True(t, ok)
//
//	transformer, warnings, err := def.Instance(
//		context.Background(),
//		driver,
//		params,
//		nil,
//		"",
//	)
//	require.NoError(t, err)
//	require.Empty(t, warnings)
//
//	r, err := transformer.Transformer.Transform(
//		context.Background(),
//		record,
//	)
//	require.NoError(t, err)
//
//	rawVal, err := r.GetRawColumnValueByName(columnName)
//	require.NoError(t, err)
//	require.False(t, rawVal.IsNull)
//	log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
//	totalNames := append(transformers.DefaultFirstNamesFemale, transformers.DefaultFirstNamesMale...)
//	require.True(t, slices.Contains(totalNames, string(rawVal.Data)))
//}
//
//func TestRandomPersonTransformer_Transform_static_lastname(t *testing.T) {
//
//	columnName := "data"
//	originalValue := "John Dust123"
//	params := map[string]toolkit.ParamsValue{
//		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .LastName }}"}]`),
//		"engine":  toolkit.ParamsValue("random"),
//		"gender":  toolkit.ParamsValue("Any"),
//	}
//
//	driver, record := getDriverAndRecord(columnName, originalValue)
//	def, ok := utils.DefaultTransformerRegistry.Get("RandomPerson")
//	require.True(t, ok)
//
//	transformer, warnings, err := def.Instance(
//		context.Background(),
//		driver,
//		params,
//		nil,
//		"",
//	)
//	require.NoError(t, err)
//	require.Empty(t, warnings)
//
//	r, err := transformer.Transformer.Transform(
//		context.Background(),
//		record,
//	)
//	require.NoError(t, err)
//
//	rawVal, err := r.GetRawColumnValueByName(columnName)
//	require.NoError(t, err)
//	require.False(t, rawVal.IsNull)
//	log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
//	require.True(t, slices.Contains(transformers.DefaultLastNames, string(rawVal.Data)))
//}
//
//func TestRandomPersonTransformer_Transform_static_nullable(t *testing.T) {
//	columnName := "data"
//	originalValue := "\\N"
//	params := map[string]toolkit.ParamsValue{
//		"columns": toolkit.ParamsValue(`[{"name": "data", "template": "{{ .Title }} {{ .FirstName }} {{ .LastName }}"}]`),
//		"engine":  toolkit.ParamsValue("hash"),
//		"gender":  toolkit.ParamsValue("Any"),
//	}
//
//	driver, record := getDriverAndRecord(columnName, originalValue)
//	def, ok := utils.DefaultTransformerRegistry.Get("RandomPerson")
//	require.True(t, ok)
//
//	transformer, warnings, err := def.Instance(
//		context.Background(),
//		driver,
//		params,
//		nil,
//		"",
//	)
//	require.NoError(t, err)
//	require.Empty(t, warnings)
//
//	r, err := transformer.Transformer.Transform(
//		context.Background(),
//		record,
//	)
//	require.NoError(t, err)
//
//	rawVal, err := r.GetRawColumnValueByName(columnName)
//	require.NoError(t, err)
//	require.True(t, rawVal.IsNull)
//}
//
//func TestRandomPersonTransformer_Transform_keep_null(t *testing.T) {
//	t.Run("keep_null for all columns", func(t *testing.T) {
//		originalValue := "\\N\t\\N"
//		params := map[string]toolkit.ParamsValue{
//			"columns": toolkit.ParamsValue(`
//			[
//				{
//					"name": "first_name",
//					"template": "{{ .FirstName }}",
//					"keep_null": true
//				},
//				{
//					"name": "last_name",
//					"template": "{{ .LastName }}",
//					"keep_null": true
//				}
//			]`,
//			),
//			"engine": toolkit.ParamsValue("hash"),
//			"gender": toolkit.ParamsValue("Any"),
//		}
//
//		driver, record := getDriverAndRecordByColumns([]string{"first_name", "last_name"}, originalValue)
//		def, ok := utils.DefaultTransformerRegistry.Get("RandomPerson")
//		require.True(t, ok)
//
//		transformer, warnings, err := def.Instance(
//			context.Background(),
//			driver,
//			params,
//			nil,
//			"",
//		)
//		require.NoError(t, err)
//		require.Empty(t, warnings)
//
//		r, err := transformer.Transformer.Transform(
//			context.Background(),
//			record,
//		)
//		require.NoError(t, err)
//
//		fistNameRawValue, err := r.GetRawColumnValueByName("first_name")
//		require.NoError(t, err)
//		require.True(t, fistNameRawValue.IsNull)
//
//		lastNameRawValue, err := r.GetRawColumnValueByName("last_name")
//		require.NoError(t, err)
//		require.True(t, lastNameRawValue.IsNull)
//	})
//
//	t.Run("keep_null only for one column", func(t *testing.T) {
//		originalValue := "\\N\t\\N"
//		params := map[string]toolkit.ParamsValue{
//			"columns": toolkit.ParamsValue(`
//			[
//				{
//					"name": "first_name",
//					"template": "{{ .FirstName }}",
//					"keep_null": false
//				},
//				{
//					"name": "last_name",
//					"template": "{{ .LastName }}",
//					"keep_null": true
//				}
//			]`,
//			),
//			"engine": toolkit.ParamsValue("hash"),
//			"gender": toolkit.ParamsValue("Any"),
//		}
//
//		driver, record := getDriverAndRecordByColumns([]string{"first_name", "last_name"}, originalValue)
//		def, ok := utils.DefaultTransformerRegistry.Get("RandomPerson")
//		require.True(t, ok)
//
//		transformer, warnings, err := def.Instance(
//			context.Background(),
//			driver,
//			params,
//			nil,
//			"",
//		)
//		require.NoError(t, err)
//		require.Empty(t, warnings)
//
//		r, err := transformer.Transformer.Transform(
//			context.Background(),
//			record,
//		)
//		require.NoError(t, err)
//
//		fistNameRawValue, err := r.GetRawColumnValueByName("first_name")
//		require.NoError(t, err)
//		require.False(t, fistNameRawValue.IsNull)
//
//		lastNameRawValue, err := r.GetRawColumnValueByName("last_name")
//		require.NoError(t, err)
//		require.True(t, lastNameRawValue.IsNull)
//	})
//
//	t.Run("keep_null false for all columns and hash two times", func(t *testing.T) {
//		originalValue := "\\N\t\\N"
//		params := map[string]toolkit.ParamsValue{
//			"columns": toolkit.ParamsValue(`
//			[
//				{
//					"name": "first_name",
//					"template": "{{ .FirstName }}",
//					"keep_null": false,
//					"hashing": true
//				},
//				{
//					"name": "last_name",
//					"template": "{{ .LastName }}",
//					"keep_null": false,
//					"hashing": true
//				}
//			]`,
//			),
//			"engine": toolkit.ParamsValue("hash"),
//			"gender": toolkit.ParamsValue("Any"),
//		}
//
//		driver, record := getDriverAndRecordByColumns([]string{"first_name", "last_name"}, originalValue)
//		def, ok := utils.DefaultTransformerRegistry.Get("RandomPerson")
//		require.True(t, ok)
//
//		transformer, warnings, err := def.Instance(
//			context.Background(),
//			driver,
//			params,
//			nil,
//			"",
//		)
//		require.NoError(t, err)
//		require.Empty(t, warnings)
//
//		r, err := transformer.Transformer.Transform(
//			context.Background(),
//			record,
//		)
//		require.NoError(t, err)
//
//		fistNameRawValue, err := r.GetRawColumnValueByName("first_name")
//		require.NoError(t, err)
//		require.False(t, fistNameRawValue.IsNull)
//		fistName := string(fistNameRawValue.Data)
//
//		lastNameRawValue, err := r.GetRawColumnValueByName("last_name")
//		require.NoError(t, err)
//		require.False(t, lastNameRawValue.IsNull)
//		lastName := string(lastNameRawValue.Data)
//
//		_, record = getDriverAndRecordByColumns([]string{"first_name", "last_name"}, originalValue)
//
//		r, err = transformer.Transformer.Transform(
//			context.Background(),
//			record,
//		)
//		require.NoError(t, err)
//
//		fistNameRawValue, err = r.GetRawColumnValueByName("first_name")
//		require.NoError(t, err)
//		require.False(t, fistNameRawValue.IsNull)
//		assert.Equal(t, fistName, string(fistNameRawValue.Data))
//
//		lastNameRawValue, err = r.GetRawColumnValueByName("last_name")
//		require.NoError(t, err)
//		require.False(t, lastNameRawValue.IsNull)
//		assert.Equal(t, lastName, string(lastNameRawValue.Data))
//	})
//}
