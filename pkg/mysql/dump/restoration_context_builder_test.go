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

package dump

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// helpers

func makeSpec(taskID core.TaskID, objectID core.ObjectID) core.ObjectDumpSpec {
	return core.ObjectDumpSpec{
		TaskID:   taskID,
		ObjectID: objectID,
		Kind:     core.ObjectKindTable,
	}
}

func makeObjectGraph(nodes []core.ObjectID, edges map[core.ObjectID][]core.ObjectID) core.ObjectGraph {
	g := core.ObjectGraph{
		Nodes: make(map[core.ObjectID]core.ObjectNode, len(nodes)),
		Edges: make(map[core.ObjectID][]core.ObjectEdge),
	}
	for _, id := range nodes {
		g.Nodes[id] = core.ObjectNode{ID: id}
	}
	for from, tos := range edges {
		for _, to := range tos {
			g.Edges[from] = append(g.Edges[from], core.ObjectEdge{From: from, To: to})
		}
	}
	return g
}

// buildInput constructs a RestorationContextInput with no FK cycles among specs.
func buildInput(specs []core.ObjectDumpSpec, graph core.ObjectGraph) core.RestorationContextInput {
	return core.RestorationContextInput{
		DumpContext: core.DumpContext{
			DumpObjectSpecs: specs,
		},
		DependencyGraph: core.DependencyGraphResult{
			ObjectGraph:    graph,
			CondensedGraph: core.CondensedGraph{Nodes: map[core.SCCID]core.SCCNode{}},
		},
	}
}

// buildCyclicInput constructs a RestorationContextInput where the given object IDs
// form a single SCC with cycles (simulating an FK cycle detected by the graphbuilder).
func buildCyclicInput(specs []core.ObjectDumpSpec, graph core.ObjectGraph, cyclicMembers []core.ObjectID) core.RestorationContextInput {
	cycleEdges := make([]core.ObjectEdge, 0, len(cyclicMembers))
	for _, id := range cyclicMembers {
		for _, e := range graph.Edges[id] {
			cycleEdges = append(cycleEdges, e)
		}
	}
	sccNode := core.SCCNode{
		ID:      0,
		Members: cyclicMembers,
		Cycles:  &core.CycleGraph{Cycles: []core.Cycle{{ID: "c0", Edges: cycleEdges}}},
	}
	condensed := core.CondensedGraph{
		Nodes: map[core.SCCID]core.SCCNode{0: sccNode},
	}
	return core.RestorationContextInput{
		DumpContext: core.DumpContext{
			DumpObjectSpecs: specs,
		},
		DependencyGraph: core.DependencyGraphResult{
			ObjectGraph:    graph,
			CondensedGraph: condensed,
		},
	}
}

// tests

func TestRestorationContextBuilder_EmptySpecs(t *testing.T) {
	b := &RestorationContextBuilder{}
	rc, err := b.Build(context.Background(), core.RestorationContextInput{})
	require.NoError(t, err)
	assert.Equal(t, core.RestorationContext{}, rc)
}

func TestRestorationContextBuilder_TwoIndependentTables(t *testing.T) {
	const (
		taskA core.TaskID   = 1
		taskB core.TaskID   = 2
		oidA  core.ObjectID = 10
		oidB  core.ObjectID = 20
	)

	specs := []core.ObjectDumpSpec{makeSpec(taskA, oidA), makeSpec(taskB, oidB)}
	graph := makeObjectGraph([]core.ObjectID{oidA, oidB}, nil)

	rc, err := new(RestorationContextBuilder).Build(context.Background(), buildInput(specs, graph))
	require.NoError(t, err)

	assert.True(t, rc.HasTopologicalOrder)
	assert.ElementsMatch(t, []core.TaskID{taskA, taskB}, rc.RestorationOrder)
	assert.Equal(t, []core.TaskID{}, rc.TaskDependencies[taskA])
	assert.Equal(t, []core.TaskID{}, rc.TaskDependencies[taskB])
}

func TestRestorationContextBuilder_LinearDependency(t *testing.T) {
	// A depends on B (A has FK → B). Restore order must be B, A.
	const (
		taskA core.TaskID   = 1
		taskB core.TaskID   = 2
		oidA  core.ObjectID = 10
		oidB  core.ObjectID = 20
	)

	specs := []core.ObjectDumpSpec{makeSpec(taskA, oidA), makeSpec(taskB, oidB)}
	graph := makeObjectGraph(
		[]core.ObjectID{oidA, oidB},
		map[core.ObjectID][]core.ObjectID{oidA: {oidB}},
	)

	rc, err := new(RestorationContextBuilder).Build(context.Background(), buildInput(specs, graph))
	require.NoError(t, err)

	assert.True(t, rc.HasTopologicalOrder)
	assert.Equal(t, []core.TaskID{taskB, taskA}, rc.RestorationOrder)
	assert.Equal(t, []core.TaskID{taskB}, rc.TaskDependencies[taskA])
	assert.Equal(t, []core.TaskID{}, rc.TaskDependencies[taskB])
}

func TestRestorationContextBuilder_Chain(t *testing.T) {
	// A→B→C. Restore order: C, B, A.
	const (
		taskA core.TaskID   = 1
		taskB core.TaskID   = 2
		taskC core.TaskID   = 3
		oidA  core.ObjectID = 10
		oidB  core.ObjectID = 20
		oidC  core.ObjectID = 30
	)

	specs := []core.ObjectDumpSpec{makeSpec(taskA, oidA), makeSpec(taskB, oidB), makeSpec(taskC, oidC)}
	graph := makeObjectGraph(
		[]core.ObjectID{oidA, oidB, oidC},
		map[core.ObjectID][]core.ObjectID{oidA: {oidB}, oidB: {oidC}},
	)

	rc, err := new(RestorationContextBuilder).Build(context.Background(), buildInput(specs, graph))
	require.NoError(t, err)

	assert.True(t, rc.HasTopologicalOrder)
	assert.Equal(t, []core.TaskID{taskC, taskB, taskA}, rc.RestorationOrder)
	assert.Equal(t, []core.TaskID{taskB}, rc.TaskDependencies[taskA])
	assert.Equal(t, []core.TaskID{taskC}, rc.TaskDependencies[taskB])
	assert.Equal(t, []core.TaskID{}, rc.TaskDependencies[taskC])
}

func TestRestorationContextBuilder_Cycle(t *testing.T) {
	// A→B and B→A (FK cycle). HasTopologicalOrder must be false; both tasks still present.
	const (
		taskA core.TaskID   = 1
		taskB core.TaskID   = 2
		oidA  core.ObjectID = 10
		oidB  core.ObjectID = 20
	)

	specs := []core.ObjectDumpSpec{makeSpec(taskA, oidA), makeSpec(taskB, oidB)}
	graph := makeObjectGraph(
		[]core.ObjectID{oidA, oidB},
		map[core.ObjectID][]core.ObjectID{oidA: {oidB}, oidB: {oidA}},
	)
	input := buildCyclicInput(specs, graph, []core.ObjectID{oidA, oidB})

	rc, err := new(RestorationContextBuilder).Build(context.Background(), input)
	require.NoError(t, err)

	assert.False(t, rc.HasTopologicalOrder)
	assert.Nil(t, rc.RestorationOrder) // no valid topological order exists
	assert.Equal(t, []core.TaskID{taskB}, rc.TaskDependencies[taskA])
	assert.Equal(t, []core.TaskID{taskA}, rc.TaskDependencies[taskB])
}

func TestRestorationContextBuilder_FilteredTable(t *testing.T) {
	// oidC is in the object graph but not in DumpObjectSpecs (filtered out).
	// Its edges must not affect the output.
	const (
		taskA core.TaskID   = 1
		taskB core.TaskID   = 2
		oidA  core.ObjectID = 10
		oidB  core.ObjectID = 20
		oidC  core.ObjectID = 30 // filtered out
	)

	specs := []core.ObjectDumpSpec{makeSpec(taskA, oidA), makeSpec(taskB, oidB)}
	// A→C (C is out of scope), B has no edges.
	graph := makeObjectGraph(
		[]core.ObjectID{oidA, oidB, oidC},
		map[core.ObjectID][]core.ObjectID{oidA: {oidC}},
	)

	rc, err := new(RestorationContextBuilder).Build(context.Background(), buildInput(specs, graph))
	require.NoError(t, err)

	assert.True(t, rc.HasTopologicalOrder)
	assert.ElementsMatch(t, []core.TaskID{taskA, taskB}, rc.RestorationOrder)
	// A's edge to C is dropped because C is out of scope.
	assert.Equal(t, []core.TaskID{}, rc.TaskDependencies[taskA])
	assert.Equal(t, []core.TaskID{}, rc.TaskDependencies[taskB])
}
