// Package tableprinter configures the text tables that greenmask commands print.
package tableprinter

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"golang.org/x/term"
)

// WrapWidth is the width at which wrapped cells break.
const WrapWidth = 30

var wordRegexp = regexp.MustCompile(`[^ \n]+`)

// New returns a table with an ASCII border, centered upper-case headers and numbers aligned right.
func New() table.Writer {
	t := table.NewWriter()
	t.SetStyle(table.StyleDefault)
	t.Style().Format.HeaderAlign = text.AlignCenter
	t.Style().Format.RowAlign = text.AlignAuto
	return t
}

// Render writes the table to w.
func Render(w io.Writer, t table.Writer) error {
	if _, err := fmt.Fprintln(w, t.Render()); err != nil {
		return fmt.Errorf("error writing table: %w", err)
	}
	return nil
}

// Row wraps every cell at WrapWidth. go-pretty's WidthMax would cut a long word, so cells are wrapped before.
func Row(record []string) table.Row {
	row := make(table.Row, len(record))
	for idx, v := range record {
		row[idx] = Wrap(v, WrapWidth)
	}
	return row
}

// AppendRows appends the records wrapped like Row and merges equal cells in the merged columns, numbered from 1.
// It replaces the column configs of t. Blank cells are not merged.
func AppendRows(t table.Writer, data [][]string, merged ...int) {
	configs := make([]table.ColumnConfig, len(merged))
	for idx, number := range merged {
		configs[idx] = table.ColumnConfig{Number: number, AutoMerge: true}
	}
	t.SetColumnConfigs(configs)

	var prev []string
	for _, record := range data {
		record = slices.Clone(record)
		for _, number := range merged {
			col := number - 1
			// go-pretty merges equal blank cells too, so a space keeps them apart.
			if prev != nil && strings.TrimSpace(record[col]) == "" && record[col] == prev[col] {
				record[col] += " "
			}
		}
		t.AppendRow(Row(record))
		prev = record
	}
}

// Wrap greedily breaks s on single spaces into lines of at most width cells, never splitting a word and keeping
// runs of spaces. When a line of s is wider than width, all its lines are joined and wrapped again.
func Wrap(s string, width int) string {
	lines := strings.Split(s, "\n")
	if !slices.ContainsFunc(lines, func(line string) bool {
		return text.StringWidthWithoutEscSequences(line) > width
	}) {
		return s
	}
	var b strings.Builder
	lineLen := 0
	for idx, word := range strings.Split(strings.ReplaceAll(s, "\n", " "), " ") {
		wordLen := text.StringWidthWithoutEscSequences(word)
		// An empty word comes from a run of spaces and never starts a new line.
		switch {
		case idx == 0:
		case wordLen > 0 && lineLen > 0 && lineLen+1+wordLen > width:
			b.WriteByte('\n')
			lineLen = 0
		default:
			b.WriteByte(' ')
			lineLen++
		}
		b.WriteString(word)
		lineLen += wordLen
	}
	return b.String()
}

// CutLongWords cuts every word of s longer than n runes into lines of n runes and keeps the spaces as they are.
func CutLongWords(s string, n int) string {
	return wordRegexp.ReplaceAllStringFunc(s, func(word string) string {
		runes := []rune(word)
		var parts []string
		for len(runes) > n {
			parts = append(parts, string(runes[:n]))
			runes = runes[n:]
		}
		return strings.Join(append(parts, string(runes)), "\n")
	})
}

// Colorize colors each line of s on its own, so a wrapped cell keeps its color on every line.
func Colorize(colors text.Colors, s string) string {
	if len(colors) == 0 {
		return s
	}
	lines := strings.Split(s, "\n")
	for idx, line := range lines {
		lines[idx] = colors.Sprint(line)
	}
	return strings.Join(lines, "\n")
}

// SetupColors turns colors off when f is not a terminal, and on when FORCE_COLOR is set.
// NO_COLOR and TERM=dumb are handled by go-pretty itself.
func SetupColors(f *os.File) {
	if forceColor := os.Getenv("FORCE_COLOR"); forceColor != "" && forceColor != "0" && forceColor != "false" {
		// go-pretty ignores FORCE_COLOR when TERM=dumb.
		text.EnableColors()
		return
	}
	if !term.IsTerminal(int(f.Fd())) {
		text.DisableColors()
	}
}
