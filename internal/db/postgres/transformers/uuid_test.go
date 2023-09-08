package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

func TestUuidTransformer_Transform_uuid_type(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
	}{
		{
			name:       "text",
			columnName: "data",
		},
		{
			name:       "uuid",
			columnName: "uid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(tt.columnName, toolkit.DefaultNullSeq)

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
				record,
			)
			require.NoError(t, err)
			res, err := r.EncodeAttr(tt.columnName)
			require.NoError(t, err)

			assert.NoError(t, err)
			assert.Regexp(t, `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`, string(res))
		})
	}
}
