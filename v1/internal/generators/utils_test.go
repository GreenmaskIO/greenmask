package generators

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildBytesFromInt(t *testing.T) {
	value := int64(123)
	intBytes := BuildBytesFromInt64(value)[:3]
	res := BuildInt64FromBytes(intBytes)
	require.Equal(t, value, res)
}
