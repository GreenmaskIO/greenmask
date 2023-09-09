package transformers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

func TestUuidTransformer_Transform_uuid_type(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		params     map[string][]byte
		original   string
		regexp     string
	}{
		{
			name:       "text",
			columnName: "data",
			params:     map[string][]byte{},
			original:   "someval",
			regexp:     `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "uuid",
			columnName: "uid",
			original:   "ddfb6f74-1771-45b0-b258-ae6fcd42f505",
			params:     map[string][]byte{},
			regexp:     `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "keepNull false and NULL seq",
			columnName: "uid",
			original:   toolkit.DefaultNullSeq,
			params: map[string][]byte{
				"keepNull": []byte("false"),
			},
			regexp: `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "keepNull true and NULL seq",
			columnName: "uid",
			original:   toolkit.DefaultNullSeq,
			params: map[string][]byte{
				"keepNull": []byte("true"),
			},
			regexp: fmt.Sprintf(`^(\%s)$`, toolkit.DefaultNullSeq),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = []byte(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, toolkit.DefaultNullSeq)
			transformer, warnings, err := RandomUuidTransformerDefinition.Instance(
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

			assert.NoError(t, err)
			assert.Regexp(t, tt.regexp, string(res))
		})
	}
}
