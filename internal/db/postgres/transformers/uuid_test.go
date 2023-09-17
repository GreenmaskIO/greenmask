package transformers

import (
	"context"
	"fmt"
	"github.com/greenmaskio/greenmask/internal/domains"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

func TestUuidTransformer_Transform_uuid_type(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		params     map[string]domains.ParamsValue
		original   string
		regexp     string
	}{
		{
			name:       "text",
			columnName: "data",
			params:     map[string]domains.ParamsValue{},
			original:   "someval",
			regexp:     `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "uuid",
			columnName: "uid",
			original:   "ddfb6f74-1771-45b0-b258-ae6fcd42f505",
			params:     map[string]domains.ParamsValue{},
			regexp:     `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "keepNull false and NULL seq",
			columnName: "uid",
			original:   toolkit.DefaultNullSeq,
			params: map[string]domains.ParamsValue{
				"keep_null": domains.ParamsValue("false"),
			},
			regexp: `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "keepNull true and NULL seq",
			columnName: "uid",
			original:   toolkit.DefaultNullSeq,
			params: map[string]domains.ParamsValue{
				"keep_null": domains.ParamsValue("true"),
			},
			regexp: fmt.Sprintf(`^(\%s)$`, toolkit.DefaultNullSeq),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = domains.ParamsValue(tt.columnName)
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
