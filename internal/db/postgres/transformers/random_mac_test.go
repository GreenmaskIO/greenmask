package transformers

import (
	"context"
	"fmt"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRandomMacTransformer_Transform_random(t *testing.T) {

	tests := []struct {
		name           string
		columnName     string
		original       []byte
		params         map[string]toolkit.ParamsValue
		castType       string
		managementType string
	}{
		{
			name:       "Random mac addr with keepOriginalVendor with Universal and Individual",
			columnName: "data",
			original:   []byte("00:1a:2b:3c:4d:5e"),
			params: map[string]toolkit.ParamsValue{
				"engine":               toolkit.ParamsValue("hash"),
				"keep_original_vendor": toolkit.ParamsValue("true"),
			},
			managementType: managementTypeNameAny,
			castType:       castTypeNameAny,
		},
		{
			name:       "Random mac addr with keepOriginalVendor with Universal and Group",
			columnName: "data",
			original:   []byte("01:1a:2b:3c:4d:5e"),
			params: map[string]toolkit.ParamsValue{
				"engine":               toolkit.ParamsValue("hash"),
				"keep_original_vendor": toolkit.ParamsValue("true"),
			},
			managementType: managementTypeNameAny,
			castType:       castTypeNameAny,
		},
		{
			name:       "Random mac addr with keepOriginalVendor with Local and Group",
			columnName: "data",
			original:   []byte("03:1a:2b:3c:4d:5e"),
			params: map[string]toolkit.ParamsValue{
				"engine":               toolkit.ParamsValue("hash"),
				"keep_original_vendor": toolkit.ParamsValue("true"),
			},
			managementType: managementTypeNameAny,
			castType:       castTypeNameAny,
		},
		{
			name:       "Random mac addr without keepOriginalVendor with Universal and Group",
			columnName: "data",
			original:   []byte("03:1a:2b:3c:4d:5e"),
			params: map[string]toolkit.ParamsValue{
				"engine":               toolkit.ParamsValue("hash"),
				"keep_original_vendor": toolkit.ParamsValue("false"),
			},
			managementType: managementTypeNameUniversal,
			castType:       castTypeNameGroup,
		},
		{
			name:       "Random mac addr without keepOriginalVendor with Universal and Individual",
			columnName: "data",
			original:   []byte("03:1a:2b:3c:4d:5e"),
			params: map[string]toolkit.ParamsValue{
				"engine":               toolkit.ParamsValue("hash"),
				"keep_original_vendor": toolkit.ParamsValue("false"),
			},
			managementType: managementTypeNameUniversal,
			castType:       castTypeNameIndividual,
		},
		{
			name:       "Random mac addr without keepOriginalVendor with Local and Individual",
			columnName: "data",
			original:   []byte("03:1a:2b:3c:4d:5e"),
			params: map[string]toolkit.ParamsValue{
				"engine":               toolkit.ParamsValue("hash"),
				"keep_original_vendor": toolkit.ParamsValue("false"),
			},
			managementType: managementTypeNameLocal,
			castType:       castTypeNameIndividual,
		},
		{
			name:       "Random mac addr without keepOriginalVendor with Universal and Individual",
			columnName: "data",
			original:   []byte("03:1a:2b:3c:4d:5e"),
			params: map[string]toolkit.ParamsValue{
				"engine":               toolkit.ParamsValue("hash"),
				"keep_original_vendor": toolkit.ParamsValue("false"),
			},
			managementType: managementTypeNameLocal,
			castType:       castTypeNameGroup,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedMacOriginal, err := transformers.ParseMacAddr(tt.original)
			require.NoError(t, err)
			require.NotEmpty(t, parsedMacOriginal)

			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			tt.params["cast_type"] = toolkit.ParamsValue(tt.castType)
			tt.params["management_type"] = toolkit.ParamsValue(tt.managementType)

			driver, record := getDriverAndRecord(tt.columnName, string(tt.original))
			def, ok := utils.DefaultTransformerRegistry.Get("RandomMac")
			require.True(t, ok)

			transformer, warnings, err := def.Instance(
				context.Background(),
				driver,
				tt.params,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformer.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)
			var res []byte
			isNull, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.NoError(t, err)
			require.False(t, isNull)

			parsedMacGenerated, err := transformers.ParseMacAddr(res)
			require.NoError(t, err)

			if string(tt.params["keep_original_vendor"]) == "true" {
				require.True(
					t,
					parsedMacOriginal.CastType == parsedMacGenerated.CastType && parsedMacOriginal.ManagementType == parsedMacGenerated.ManagementType,
					fmt.Sprintf("Mac address info is't equals %+v != %+v", parsedMacOriginal, parsedMacGenerated),
				)
			}

			if tt.castType != castTypeNameAny && tt.managementType != managementTypeNameAny {
				require.True(
					t,
					castTypeNameToIndex(tt.castType) == parsedMacGenerated.CastType && managementTypeNameToIndex(tt.managementType) == parsedMacGenerated.ManagementType,
					fmt.Sprintf("Mac address info is't equals %+v != %+v", parsedMacOriginal, parsedMacGenerated),
				)
			}
		})
	}
}

func castTypeNameToIndex(catTypeName string) (catTypeIndex int) {
	switch catTypeName {
	case castTypeNameAny:
		return transformers.CastTypeAny
	case castTypeNameGroup:
		return transformers.CastTypeGroup
	case castTypeNameIndividual:
		return transformers.CastTypeIndividual
	default:
		return transformers.CastTypeAny
	}
}

func managementTypeNameToIndex(managementTypeName string) (catTypeIndex int) {
	switch managementTypeName {
	case managementTypeNameAny:
		return transformers.ManagementTypeAny
	case managementTypeNameLocal:
		return transformers.ManagementTypeLocal
	case managementTypeNameUniversal:
		return transformers.ManagementTypeUniversal
	default:
		return transformers.ManagementTypeAny
	}
}
