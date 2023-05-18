package transformers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestSetNullTransformer_Transform(t *testing.T) {
	transformer, err := NewSetNullTransformer(domains.ColumnMeta{}, nil, nil)
	require.NoError(t, err)
	res, err := transformer.Transform("old_val")
	assert.Equal(t, `\N`, res)

	transformer, err = NewSetNullTransformer(domains.ColumnMeta{}, nil, map[string]string{"nullSequence": "\\A"})
	require.NoError(t, err)
	res, err = transformer.Transform("old_val")
	assert.Equal(t, `\A`, res)
}
