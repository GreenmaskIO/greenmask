package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

func TestMaskingTransformer_Transform(t *testing.T) {
	driver := getDriver()

	tests := []struct {
		name           string
		ttype          string
		originalRecord []string
		expectedRecord []string
		idx            int
	}{
		{
			name:           "mobile",
			ttype:          "mobile",
			originalRecord: []string{toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, "+35798665784"},
			expectedRecord: []string{toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, "+357***65784"},
			idx:            2,
		},
		{
			name:           "name",
			ttype:          "name",
			originalRecord: []string{toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, "abcdef test"},
			expectedRecord: []string{toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, "a**def t**t"},
			idx:            2,
		},
		{
			name:           "password",
			ttype:          "password",
			originalRecord: []string{toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, "password_secure"},
			expectedRecord: []string{toolkit.DefaultNullSeq, toolkit.DefaultNullSeq, "************"},
			idx:            2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, warnings, err := MaskingTransformerDefinition.Instance(
				context.Background(),
				driver, map[string][]byte{
					"column": []byte("title"),
					"type":   []byte(tt.ttype),
				},
				nil,
			)
			require.NoError(t, err)
			assert.Empty(t, warnings)

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

			require.Equal(t, tt.expectedRecord[tt.idx], res[tt.idx])
		})
	}
}

func TestMaskingTransformer_type_validation(t *testing.T) {
	driver := getDriver()

	_, warnings, err := MaskingTransformerDefinition.Instance(
		context.Background(),
		driver, map[string][]byte{
			"column": []byte("title"),
			"type":   []byte("unknown"),
		},
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0].Msg, "unknown type")
}
