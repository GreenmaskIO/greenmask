package transformers

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRandomBoolTransformer_Transform(t *testing.T) {

	tests := []struct {
		name          string
		params        map[string][]byte
		columnName    string
		originalValue string
		pattern       string
	}{
		{
			name:       "test bool type",
			columnName: "col_bool",
			params:     map[string][]byte{},
			pattern:    `^(t|f)$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = []byte(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
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
