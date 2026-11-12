package tableprinter

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrap(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		width int
		want  string
	}{
		{name: "short", in: "a b c", width: 5, want: "a b c"},
		{name: "empty", in: "", width: 5, want: ""},
		{name: "space", in: " ", width: 5, want: " "},
		{name: "short lines are kept", in: "ab\ncd", width: 5, want: "ab\ncd"},
		{name: "wrap on spaces", in: "aaa bbb ccc", width: 7, want: "aaa bbb\nccc"},
		{name: "long word is not split", in: "aaaaaaaaaa b", width: 5, want: "aaaaaaaaaa\nb"},
		{name: "wide lines are reflowed", in: "aaa bbb ccc\nddd", width: 7, want: "aaa bbb\nccc ddd"},
		{name: "width is counted in cells", in: "ааа ббб ввв", width: 7, want: "ааа ббб\nввв"},
		{name: "runs of spaces are kept", in: "aaaa  bbbb cccc", width: 11, want: "aaaa  bbbb\ncccc"},
		{name: "tabs stay in the word", in: "aaaa\tbbbb cccc", width: 10, want: "aaaa\tbbbb\ncccc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Wrap(tt.in, tt.width))
		})
	}
}

func TestColorize(t *testing.T) {
	text.EnableColors()
	got := Colorize(text.Colors{text.FgHiRed}, "a\nb")
	assert.Equal(t, "\x1b[91ma\x1b[0m\n\x1b[91mb\x1b[0m", got)
	assert.Equal(t, "a\nb", Colorize(nil, "a\nb"))

	text.DisableColors()
	defer text.EnableColors()
	assert.Equal(t, "a\nb", Colorize(text.Colors{text.FgHiRed}, "a\nb"))
}

func TestAppendRows_blank_cells_are_not_merged(t *testing.T) {
	tw := New()
	tw.Style().Options.SeparateRows = true
	data := [][]string{
		{"a", "x"},
		{"a", ""},
		{"b", ""},
		{"c", ""},
		{"c", "y"},
		{"d", " "},
		{"d", " "},
	}
	AppendRows(tw, data, 1, 2)
	var out bytes.Buffer
	require.NoError(t, Render(&out, tw))
	got := out.String()
	// Equal letters in the first column merge, so it has a separator only between different letters.
	assert.Equal(t, 1, strings.Count(got, "| a "))
	// The second column has a separator around every row, blank ones too: the borders and one between rows.
	separators := 0
	for _, line := range strings.Split(got, "\n") {
		if strings.HasSuffix(line, "-+") {
			separators++
		}
	}
	assert.Equal(t, len(data)+1, separators, got)
}

func TestRow_wraps_cells(t *testing.T) {
	assert.Equal(t, table.Row{"a", "w w w w w w w w w w w w w w w\nw "}, Row([]string{"a", strings.Repeat("w ", 16)}))
}

func TestCutLongWords(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "short words", in: "ab cd", want: "ab cd"},
		{name: "long word", in: "abcdefg", want: "abc\ndef\ng"},
		{name: "runes, not bytes", in: "ДлинноеСлово", want: "Дли\nнно\nеСл\nово"},
		{name: "spaces are kept", in: "a   abcd  b", want: "a   abc\nd  b"},
		{name: "lines are cut on their own", in: "abc\nabcd", want: "abc\nabc\nd"},
		{name: "empty", in: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, CutLongWords(tt.in, 3))
		})
	}
}

func TestSetupColors(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer func() {
		_ = r.Close()
		_ = w.Close()
		text.EnableColors()
	}()

	t.Setenv("FORCE_COLOR", "")
	text.EnableColors()
	SetupColors(w)
	assert.Equal(t, "x", text.Colors{text.FgRed}.Sprint("x"), "a pipe is not a terminal")

	t.Setenv("FORCE_COLOR", "1")
	text.DisableColors()
	SetupColors(w)
	assert.Equal(t, "\x1b[31mx\x1b[0m", text.Colors{text.FgRed}.Sprint("x"), "FORCE_COLOR keeps colors")
}
