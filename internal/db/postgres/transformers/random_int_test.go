package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

// TODO: Cover error cases
func TestRandomIntTransformer_Transform(t *testing.T) {
	driver := getDriver()

	tests := []struct {
		name           string
		columnName     string
		originalRecord []string
		expectedRegexp string
		idx            int
	}{
		{
			name:           "int2",
			columnName:     "id2",
			originalRecord: []string{toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq},
			expectedRegexp: `^\d{1,3}$`,
			idx:            5,
		},
		{
			name:           "int4",
			columnName:     "id4",
			originalRecord: []string{toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq},
			expectedRegexp: `^\d{1,3}$`,
			idx:            6,
		},
		{
			name:           "int8",
			columnName:     "id8",
			originalRecord: []string{toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq},
			expectedRegexp: `^\d{1,3}$`,
			idx:            7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				toolkit.NewRecord(
					driver,
					tt.originalRecord,
				),
			)
			require.NoError(t, err)
			res, err := r.Encode()
			require.NoError(t, err)
			require.Regexp(t, `^\d{1,3}$`, res[tt.idx])
		})
	}

}
