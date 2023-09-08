package transformers

import (
	"context"
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
	}{
		{
			name:           "int2",
			columnName:     "id2",
			originalValue:  toolkit.DefaultNullSeq,
			expectedRegexp: `^\d{1,3}$`,
		},
		{
			name:           "int4",
			columnName:     "id4",
			originalValue:  toolkit.DefaultNullSeq,
			expectedRegexp: `^\d{1,3}$`,
		},
		{
			name:           "int8",
			columnName:     "id8",
			originalValue:  toolkit.DefaultNullSeq,
			expectedRegexp: `^\d{1,3}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			transformer, warnings, err := RandomIntTransformerDefinition.Instance(
				context.Background(),
				driver, map[string][]byte{
					"column": []byte(tt.columnName),
					"min":    []byte("1"),
					"max":    []byte("100"),
				},
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
