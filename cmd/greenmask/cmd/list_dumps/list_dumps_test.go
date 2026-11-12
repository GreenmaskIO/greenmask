package list_dumps

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/utils/testutils"
)

func TestRenderDumpTable(t *testing.T) {
	rows := [][]string{
		{"1731400000000", "2026-11-12T10:00:00Z", "playground", "1.5 MiB", "300.2 KiB", "00:00:03", "true", "done",
			strings.Repeat("d", 60)},
		{"1731300000000", "", "", "", "", "", "false", "in progress", ""},
		{"1731200000000", "2026-11-10T10:00:00Z", "playground", "2.0 KiB", "1.0 KiB", "00:01:00", "false", "done",
			strings.Repeat("x", 57) + "..."},
	}
	var out bytes.Buffer
	require.NoError(t, renderDumpTable(&out, rows))
	got := out.String()
	testutils.AssertGolden(t, "list_dumps.golden", got)

	for _, r := range rows {
		for _, v := range r {
			assert.Contains(t, got, v)
		}
	}
	// Rows keep their order.
	assert.Less(t, strings.Index(got, rows[0][0]), strings.Index(got, rows[1][0]))
	assert.Less(t, strings.Index(got, rows[1][0]), strings.Index(got, rows[2][0]))
}
