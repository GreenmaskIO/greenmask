// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cyclesgraph

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

func Test_NewGraph(t *testing.T) {
	table1 := commonmodels.Table{
		Schema: "test",
		Name:   "a",
	}
	table2 := commonmodels.Table{
		Schema: "test",
		Name:   "b",
	}
	table3 := commonmodels.Table{
		Schema: "test",
		Name:   "c",
	}
	edge1 := tablegraph.NewEdge(
		10,
		false,
		tablegraph.NewTableLink(0, table1, nil, nil),
		tablegraph.NewTableLink(1, table2, nil, nil),
	)

	edge2 := tablegraph.NewEdge(
		11,
		false,
		tablegraph.NewTableLink(1, table2, nil, nil),
		tablegraph.NewTableLink(0, table1, nil, nil),
	)

	edge3 := tablegraph.NewEdge(
		12,
		false,
		tablegraph.NewTableLink(1, table2, nil, nil),
		tablegraph.NewTableLink(2, table3, nil, nil),
	)
	edge4 := tablegraph.NewEdge(
		13,
		false,
		tablegraph.NewTableLink(2, table3, nil, nil),
		tablegraph.NewTableLink(1, table2, nil, nil),
	)

	simpleGraph := map[int][]tablegraph.Edge{
		0: {edge1},
		1: {edge2, edge3},
		2: {edge4},
	}

	g := NewGraph(simpleGraph)
	// Validate Cycles
	require.Len(t, g.Cycles, 2)

	// 1st cycle
	cycle1 := g.Cycles[0]
	require.Len(t, cycle1, 2)
	require.Equal(t, 10, cycle1[0].ID())
	require.Equal(t, 11, cycle1[1].ID())

	// 2nd cycle
	cycle2 := g.Cycles[1]
	require.Len(t, cycle2, 2)
	require.Equal(t, 12, cycle2[0].ID())
	require.Equal(t, 13, cycle2[1].ID())

	// Check cycle identification
	require.Equal(t, map[string]struct{}{"10_11": {}, "12_13": {}}, g.cyclesIdents)

	// Check Cycles group
	require.Equal(t, map[string][]int{"0_1": {0}, "1_2": {1}}, g.GroupedCycles)

	// Check Cycles Graph
	require.Len(t, g.Graph, 1)
	cycleEdgesFrom0To1 := g.Graph["0_1"]
	require.Len(t, cycleEdgesFrom0To1, 1)
	cycleEdgeFrom0To1 := cycleEdgesFrom0To1[0]
	require.Equal(t, "0_1", cycleEdgeFrom0To1.from)
	require.Equal(t, "1_2", cycleEdgeFrom0To1.to)
}

func Test_getCycleGroupId(t *testing.T) {
	edge1 := tablegraph.NewEdge(
		4,
		false,
		tablegraph.NewTableLink(2, commonmodels.Table{}, nil, nil),
		tablegraph.NewTableLink(3, commonmodels.Table{}, nil, nil),
	)

	edge2 := tablegraph.NewEdge(
		5,
		false,
		tablegraph.NewTableLink(3, commonmodels.Table{}, nil, nil),
		tablegraph.NewTableLink(2, commonmodels.Table{}, nil, nil),
	)
	cycle := []tablegraph.Edge{edge1, edge2}

	expected := "2_3"
	actual := getCycleGroupId(cycle)
	require.Equal(t, actual, expected)
}

func Test_getCycleId(t *testing.T) {
	edge1 := tablegraph.NewEdge(
		5,
		false,
		tablegraph.NewTableLink(2, commonmodels.Table{}, nil, nil),
		tablegraph.NewTableLink(3, commonmodels.Table{}, nil, nil),
	)

	edge2 := tablegraph.NewEdge(
		4,
		false,
		tablegraph.NewTableLink(3, commonmodels.Table{}, nil, nil),
		tablegraph.NewTableLink(2, commonmodels.Table{}, nil, nil),
	)
	cycle := []tablegraph.Edge{edge1, edge2}

	expected := "4_5"
	actual := getCycleId(cycle)
	require.Equal(t, actual, expected)
}
