package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

func TestUuidTransformer_Transform_uuid_type(t *testing.T) {
	driver := getDriver()

	originalRecord := []string{"1", toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, toolkit.DefaultNullSeq}

	tests := []struct {
		name       string
		columnName string
		idx        int
	}{
		{
			name:       "text",
			columnName: "title",
			idx:        2,
		},
		{
			name:       "uuid",
			columnName: "uid",
			idx:        4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, warnings, err := RandomUuidTransformerDefinition.Instance(
				context.Background(),
				driver, map[string][]byte{
					"column": []byte(tt.columnName),
				},
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformer.Transform(
				context.Background(),
				toolkit.NewRecord(
					driver,
					originalRecord,
				),
			)
			require.NoError(t, err)
			res, err := r.Encode()
			require.NoError(t, err)

			assert.NoError(t, err)
			assert.Regexp(t, `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`, res[tt.idx])
		})
	}
}
