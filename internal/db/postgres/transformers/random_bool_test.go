package transformers

import (
	"context"
	"fmt"
	"github.com/greenmaskio/greenmask/internal/domains"
	"log"
	"testing"

	"github.com/stretchr/testify/require"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

func TestRandomBoolTransformer_Transform(t *testing.T) {

	tests := []struct {
		name       string
		params     map[string]domains.ParamsValue
		columnName string
		original   string
		pattern    string
	}{
		{
			name:       "common",
			original:   "t",
			columnName: "col_bool",
			params:     map[string]domains.ParamsValue{},
			pattern:    `^(t|f)$`,
		},
		{
			name:       "keep_null false and NULL seq",
			original:   toolkit.DefaultNullSeq,
			columnName: "col_bool",
			params: map[string]domains.ParamsValue{
				"keep_null": domains.ParamsValue("false"),
			},
			pattern: `^(t|f)$`,
		},
		{
			name:       "keep_null true and NULL seq",
			original:   toolkit.DefaultNullSeq,
			columnName: "col_bool",
			params: map[string]domains.ParamsValue{
				"keep_null": domains.ParamsValue("true"),
			},
			pattern: fmt.Sprintf(`^(\%s)$`, toolkit.DefaultNullSeq),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = domains.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			transformer, warnings, err := RandomBoolTransformerDefinition.Instance(
				context.Background(),
				driver,
				tt.params,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)
			res, err := r.EncodeAttr(tt.columnName)
			require.NoError(t, err)
			log.Println(res)
			require.Regexp(t, tt.pattern, string(res))
		})
	}
}
