package strings

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWrapString(t *testing.T) {
	original := "1234567890"
	maxLength := 7

	strs := strings.Split(WrapString(original, maxLength), "\n")
	require.Len(t, strs[0], 7)
	require.Len(t, strs[1], 3)
}
