package testutils

import (
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var updateGolden = flag.Bool("update", false, "rewrite the golden files under testdata")

var sgrRegexp = regexp.MustCompile(`\x1b\[([0-9;]*)m`)

// separatorLineRegexp matches a row separator, also one that passes through merged cells, such as "|    +----+".
// It needs a "+", so a data row such as "| - |" is not a separator.
var separatorLineRegexp = regexp.MustCompile(`^[ |]*\+[ |+]*-[ |+-]*$`)

var sgrColorNames = []string{"black", "red", "green", "yellow", "blue", "magenta", "cyan", "white"}

// AssertGolden compares got with testdata/name and rewrites the file when the tests run with -update.
func AssertGolden(t *testing.T, name string, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(got), 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err, "run the test with -update to create the golden file")
	assert.Equal(t, string(want), got)
}

// StripANSI removes SGR escape sequences.
func StripANSI(s string) string {
	return sgrRegexp.ReplaceAllString(s, "")
}

// NormalizeANSI replaces SGR escape sequences with readable tags such as <fg-hi-red> and <reset>.
func NormalizeANSI(s string) string {
	return sgrRegexp.ReplaceAllStringFunc(s, func(seq string) string {
		params := sgrRegexp.FindStringSubmatch(seq)[1]
		if params == "" {
			return "<reset>"
		}
		var tags []string
		for _, p := range strings.Split(params, ";") {
			tags = append(tags, "<"+sgrTagName(p)+">")
		}
		return strings.Join(tags, "")
	})
}

func sgrTagName(param string) string {
	code, err := strconv.Atoi(param)
	if err != nil {
		return "sgr-" + param
	}
	switch {
	case code == 0:
		return "reset"
	case code == 1:
		return "bold"
	case code >= 30 && code <= 37:
		return "fg-" + sgrColorNames[code-30]
	case code >= 40 && code <= 47:
		return "bg-" + sgrColorNames[code-40]
	case code >= 90 && code <= 97:
		return "fg-hi-" + sgrColorNames[code-90]
	case code >= 100 && code <= 107:
		return "bg-hi-" + sgrColorNames[code-100]
	}
	return "sgr-" + param
}

// TableCells splits the "|" lines of an ASCII table into trimmed cells, ignoring SGR sequences.
func TableCells(out string) [][]string {
	var res [][]string
	for _, line := range strings.Split(StripANSI(out), "\n") {
		if !strings.HasPrefix(line, "|") || separatorLineRegexp.MatchString(line) {
			continue
		}
		parts := strings.Split(strings.Trim(line, "|"), "|")
		cells := make([]string, len(parts))
		for idx, p := range parts {
			cells[idx] = strings.TrimSpace(p)
		}
		res = append(res, cells)
	}
	return res
}

// CountCells counts the table lines whose cell at column col equals value.
func CountCells(cells [][]string, col int, value string) int {
	count := 0
	for _, c := range cells {
		if len(c) > col && c[col] == value {
			count++
		}
	}
	return count
}
