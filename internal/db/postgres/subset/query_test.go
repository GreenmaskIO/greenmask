package subset

import (
	"testing"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/stretchr/testify/require"
)

func TestFindSubsetVertexes(t *testing.T) {
	graph := [][]*Edge{
		nil,                            // 1
		{{Idx: 0}},                     // 2
		{{Idx: 0}, {Idx: 1}},           // 3
		{{Idx: 1}},                     // 4
		{{Idx: 3}, {Idx: 7}, {Idx: 6}}, // 5
		{{Idx: 4}, {Idx: 9}},           // 6
		nil,                            // 7
		nil,                            // 8
		{{Idx: 6}},                     // 9
		nil,                            // 10
	}

	tables := []*entries.Table{
		{},                                // 1
		{SubsetConds: []string{"b = 2"}},  // 2
		{},                                // 3
		{},                                // 4
		{SubsetConds: []string{"e = 5"}},  // 5
		{},                                // 6
		{},                                // 7
		{},                                // 8
		{SubsetConds: []string{"i = 9"}},  // 9
		{SubsetConds: []string{"i = 10"}}, // 10
	}

	expected := map[int][]int{
		1: {1},
		2: {1, 2},
		3: {1, 3},
		4: {1, 3, 4},
		5: {1, 3, 4, 9, 5},
		8: {8},
		9: {9},
	}

	paths := findSubsetVertexes(graph, tables)
	require.NotEmpty(t, paths)
	require.Equal(t, expected, paths)
}
