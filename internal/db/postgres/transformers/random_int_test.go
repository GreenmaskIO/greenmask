package transformers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

func TestRandomIntTransformer_Transform(t *testing.T) {

	tests := []struct {
		name           string
		columnName     string
		originalValue  string
		expectedRegexp string
		params         map[string][]byte
	}{
		{
			name:           "int2",
			columnName:     "id2",
			originalValue:  "12345",
			expectedRegexp: `^\d{1,3}$`,
			params: map[string][]byte{
				"min": []byte("1"),
				"max": []byte("100"),
			},
		},
		{
			name:           "int4",
			columnName:     "id4",
			originalValue:  "12345",
			expectedRegexp: `^\d{1,3}$`,
			params: map[string][]byte{
				"min": []byte("1"),
				"max": []byte("100"),
			},
		},
		{
			name:           "int8",
			columnName:     "id8",
			originalValue:  "12345",
			expectedRegexp: `^\d{1,3}$`,
			params: map[string][]byte{
				"min": []byte("1"),
				"max": []byte("100"),
			},
		},
		{
			name:           "keep_null false and NULL seq",
			columnName:     "id8",
			originalValue:  toolkit.DefaultNullSeq,
			expectedRegexp: `^\d{1,3}$`,
			params: map[string][]byte{
				"min":       []byte("1"),
				"max":       []byte("100"),
				"keep_null": []byte("false"),
			},
		},
		{
			name:           "keep_null true and NULL seq",
			columnName:     "id8",
			originalValue:  toolkit.DefaultNullSeq,
			expectedRegexp: fmt.Sprintf(`^(\%s)$`, toolkit.DefaultNullSeq),
			params: map[string][]byte{
				"min":       []byte("1"),
				"max":       []byte("100"),
				"keep_null": []byte("true"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = []byte(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			transformer, warnings, err := RandomIntTransformerDefinition.Instance(
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
			require.Regexp(t, tt.expectedRegexp, string(res))
		})
	}

}
