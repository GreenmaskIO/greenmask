package transformers

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/require"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

func TestRandomBoolTransformer_Transform(t *testing.T) {

	tests := []struct {
		name       string
		params     map[string][]byte
		columnName string
		original   string
		pattern    string
	}{
		{
			name:       "common",
			original:   "t",
			columnName: "col_bool",
			params:     map[string][]byte{},
			pattern:    `^(t|f)$`,
		},
		{
			name:       "keep_null false and NULL seq",
			original:   toolkit.DefaultNullSeq,
			columnName: "col_bool",
			params: map[string][]byte{
				"keep_null": []byte("false"),
			},
			pattern: `^(t|f)$`,
		},
		{
			name:       "keep_null true and NULL seq",
			original:   toolkit.DefaultNullSeq,
			columnName: "col_bool",
			params: map[string][]byte{
				"keep_null": []byte("true"),
			},
			pattern: fmt.Sprintf(`^(\%s)$`, toolkit.DefaultNullSeq),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = []byte(tt.columnName)
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
