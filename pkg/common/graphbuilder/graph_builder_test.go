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

package graphbuilder_test

import (
	"context"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/common/graphbuilder"
	"github.com/greenmaskio/greenmask/pkg/common/models"
)

// tableObject wraps a common table as an introspection object with the given ID.
func tableObject(id models.ObjectID, t models.Table) models.Object {
	return models.Object{
		ID:      id,
		Kind:    models.ObjectKindTable,
		Name:    t.Name,
		Payload: t,
	}
}

func introspection(objs ...models.Object) models.IntrospectionResult {
	return models.IntrospectionResult{
		Engine: models.DBMSEngineMySQL,
		KindsMap: map[models.ObjectKind][]models.Object{
			models.ObjectKindTable: objs,
		},
	}
}

func findEdge(edges []models.ObjectEdge, to models.ObjectID) (models.ObjectEdge, bool) {
	for _, e := range edges {
		if e.To == to {
			return e, true
		}
	}
	return models.ObjectEdge{}, false
}

func findCondensedEdge(cg models.CondensedGraph, from, to models.SCCID) (models.SCCEdge, bool) {
	for _, e := range cg.Edges[from] {
		if e.To == to {
			return e, true
		}
	}
	return models.SCCEdge{}, false
}

func TestGraphBuilder_BuildGraph(t *testing.T) {
	ctx := context.Background()

	t.Run("dag of foreign keys", func(t *testing.T) {
		/*
			a -> b -> c   (edges point from child/referencing to parent/referenced)
		*/
		tableA := models.Table{
			Schema:     "public",
			Name:       "a",
			PrimaryKey: []string{"id"},
			References: []models.Reference{
				{
					ReferencedSchema: "public",
					ReferencedName:   "b",
					ConstraintName:   "fk_a_b",
					Keys:             []string{"b_id"},
					IsNullable:       false,
				},
			},
		}
		tableB := models.Table{
			Schema:     "public",
			Name:       "b",
			PrimaryKey: []string{"id"},
			References: []models.Reference{
				{
					ReferencedSchema: "public",
					ReferencedName:   "c",
					ConstraintName:   "fk_b_c",
					Keys:             []string{"c_id"},
					IsNullable:       true,
				},
			},
		}
		tableC := models.Table{
			Schema:     "public",
			Name:       "c",
			PrimaryKey: []string{"id"},
		}

		const (
			idA models.ObjectID = 100
			idB models.ObjectID = 101
			idC models.ObjectID = 102
		)
		in := introspection(
			tableObject(idA, tableA),
			tableObject(idB, tableB),
			tableObject(idC, tableC),
		)

		res, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)

		// Object graph: nodes keyed by introspection ObjectID, payload preserved.
		require.Len(t, res.ObjectGraph.Nodes, 3)
		require.Equal(t, "a", res.ObjectGraph.Nodes[idA].Name)
		require.Equal(t, models.ObjectKindTable, res.ObjectGraph.Nodes[idA].Kind)
		require.Equal(t, tableA, res.ObjectGraph.Nodes[idA].Payload)

		// Edge a -> b with foreign-key link payload.
		ab, ok := findEdge(res.ObjectGraph.Edges[idA], idB)
		require.True(t, ok, "expected edge a -> b")
		assert.Equal(t, models.ObjectLinkKindForeignKey, ab.Link.Kind)
		fk, ok := ab.Link.Payload.(models.ForeignKeyLinkPayload)
		require.True(t, ok, "expected ForeignKeyLinkPayload")
		assert.Equal(t, "fk_a_b", fk.ConstraintName)
		assert.Equal(t, []string{"b_id"}, fk.Columns)
		assert.Equal(t, []string{"id"}, fk.RefColumns)
		assert.False(t, fk.IsNullable)
		assert.Equal(t, []models.ObjectFieldRef{{Kind: models.FieldRefKindColumn, Value: "b_id"}}, ab.Link.From.Fields)
		assert.Equal(t, idA, ab.Link.From.ObjectID)
		assert.Equal(t, idB, ab.Link.To.ObjectID)

		// Edge b -> c is nullable.
		bc, ok := findEdge(res.ObjectGraph.Edges[idB], idC)
		require.True(t, ok, "expected edge b -> c")
		fkBC := bc.Link.Payload.(models.ForeignKeyLinkPayload)
		assert.True(t, fkBC.IsNullable)

		// c has no outgoing edges.
		assert.Empty(t, res.ObjectGraph.Edges[idC])

		// Condensation: every table is its own acyclic SCC.
		require.Len(t, res.CondensedGraph.Nodes, 3)
		require.Len(t, res.ObjectToSCC, 3)
		sccA, sccB, sccC := res.ObjectToSCC[idA], res.ObjectToSCC[idB], res.ObjectToSCC[idC]
		assert.NotEqual(t, sccA, sccB)
		assert.NotEqual(t, sccB, sccC)
		for id, scc := range res.ObjectToSCC {
			node := res.CondensedGraph.Nodes[scc]
			assert.Equal(t, []models.ObjectID{id}, node.Members)
			assert.Nil(t, node.Cycles, "acyclic SCC must not carry a cycle graph")
		}

		// Condensed edges mirror the object edges, carrying the underlying links.
		condAB, ok := findCondensedEdge(res.CondensedGraph, sccA, sccB)
		require.True(t, ok, "expected condensed edge SCC(a) -> SCC(b)")
		require.Len(t, condAB.Links, 1)
		assert.Equal(t, idA, condAB.Links[0].From)
		assert.Equal(t, idB, condAB.Links[0].To)
		_, ok = findCondensedEdge(res.CondensedGraph, sccB, sccC)
		require.True(t, ok, "expected condensed edge SCC(b) -> SCC(c)")
	})

	t.Run("cycle is condensed into a single scc", func(t *testing.T) {
		/*
			a <-> b   (mutual references form a cycle)
			c  -> a   (singleton referencing the cycle)
		*/
		tableA := models.Table{
			Schema:     "public",
			Name:       "a",
			PrimaryKey: []string{"id"},
			References: []models.Reference{
				{ReferencedSchema: "public", ReferencedName: "b", ConstraintName: "fk_a_b", Keys: []string{"b_id"}, IsNullable: true},
			},
		}
		tableB := models.Table{
			Schema:     "public",
			Name:       "b",
			PrimaryKey: []string{"id"},
			References: []models.Reference{
				{ReferencedSchema: "public", ReferencedName: "a", ConstraintName: "fk_b_a", Keys: []string{"a_id"}, IsNullable: true},
			},
		}
		tableC := models.Table{
			Schema:     "public",
			Name:       "c",
			PrimaryKey: []string{"id"},
			References: []models.Reference{
				{ReferencedSchema: "public", ReferencedName: "a", ConstraintName: "fk_c_a", Keys: []string{"a_id"}, IsNullable: false},
			},
		}

		const (
			idA models.ObjectID = 1
			idB models.ObjectID = 2
			idC models.ObjectID = 3
		)
		in := introspection(
			tableObject(idA, tableA),
			tableObject(idB, tableB),
			tableObject(idC, tableC),
		)

		res, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)

		// a and b collapse into the same SCC; c stays separate.
		sccAB := res.ObjectToSCC[idA]
		require.Equal(t, sccAB, res.ObjectToSCC[idB])
		require.NotEqual(t, sccAB, res.ObjectToSCC[idC])

		cyclic := res.CondensedGraph.Nodes[sccAB]
		assert.Equal(t, []models.ObjectID{idA, idB}, cyclic.Members) // sorted

		// The cyclic SCC exposes a cycle graph with at least one cycle.
		require.NotNil(t, cyclic.Cycles)
		require.NotEmpty(t, cyclic.Cycles.Cycles)

		// Its subgraph contains both members and the intra-component edges.
		require.Len(t, cyclic.Subgraph.Nodes, 2)
		_, ab := findEdge(cyclic.Subgraph.Edges[idA], idB)
		_, ba := findEdge(cyclic.Subgraph.Edges[idB], idA)
		assert.True(t, ab, "expected intra-SCC edge a -> b")
		assert.True(t, ba, "expected intra-SCC edge b -> a")

		// The singleton c has no cycle and an edge into the cyclic component.
		sccC := res.ObjectToSCC[idC]
		assert.Nil(t, res.CondensedGraph.Nodes[sccC].Cycles)
		_, ok := findCondensedEdge(res.CondensedGraph, sccC, sccAB)
		require.True(t, ok, "expected condensed edge SCC(c) -> SCC(a,b)")
	})

	t.Run("empty introspection yields an empty graph", func(t *testing.T) {
		res, err := graphbuilder.New().BuildGraph(ctx, introspection())
		require.NoError(t, err)
		assert.Empty(t, res.ObjectGraph.Nodes)
		assert.Empty(t, res.CondensedGraph.Nodes)
		assert.Empty(t, res.ObjectToSCC)
		// Maps are initialized (safe to index), not nil.
		assert.NotNil(t, res.ObjectGraph.Nodes)
		assert.NotNil(t, res.CondensedGraph.Nodes)
		assert.NotNil(t, res.ObjectToSCC)
	})

	t.Run("result is deterministic across runs", func(t *testing.T) {
		tableA := models.Table{Schema: "public", Name: "a", PrimaryKey: []string{"id"}}
		tableB := models.Table{
			Schema: "public", Name: "b", PrimaryKey: []string{"id"},
			References: []models.Reference{
				{ReferencedSchema: "public", ReferencedName: "a", Keys: []string{"a_id"}},
			},
		}
		in := introspection(tableObject(1, tableA), tableObject(2, tableB))

		first, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)
		second, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)
		assert.Equal(t, first, second)
	})
}

// commonTableAdapter is an engine-specific payload that exposes itself as a
// common table via ToCommonTable, mirroring how *mysql.Table is wrapped.
type commonTableAdapter struct {
	table models.Table
}

func (a commonTableAdapter) ToCommonTable() models.Table {
	return a.table
}

func TestGraphBuilder_PayloadExtraction(t *testing.T) {
	ctx := context.Background()

	t.Run("accepts ToCommonTable payloads", func(t *testing.T) {
		tableA := models.Table{Schema: "public", Name: "a", PrimaryKey: []string{"id"}}
		tableB := models.Table{
			Schema: "public", Name: "b", PrimaryKey: []string{"id"},
			References: []models.Reference{
				{ReferencedSchema: "public", ReferencedName: "a", Keys: []string{"a_id"}},
			},
		}
		payloadA := commonTableAdapter{table: tableA}
		in := models.IntrospectionResult{
			KindsMap: map[models.ObjectKind][]models.Object{
				models.ObjectKindTable: {
					{ID: 1, Kind: models.ObjectKindTable, Name: "a", Payload: payloadA},
					{ID: 2, Kind: models.ObjectKindTable, Name: "b", Payload: commonTableAdapter{table: tableB}},
				},
			},
		}

		res, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)
		require.Len(t, res.ObjectGraph.Nodes, 2)
		// The original payload (not the converted table) is preserved on the node.
		assert.Equal(t, payloadA, res.ObjectGraph.Nodes[1].Payload)
		_, ok := findEdge(res.ObjectGraph.Edges[2], 1)
		assert.True(t, ok, "expected edge b -> a from adapted payloads")
	})

	t.Run("accepts pointer-to-table payloads", func(t *testing.T) {
		tableA := models.Table{Schema: "public", Name: "a", PrimaryKey: []string{"id"}}
		in := models.IntrospectionResult{
			KindsMap: map[models.ObjectKind][]models.Object{
				models.ObjectKindTable: {
					{ID: 1, Kind: models.ObjectKindTable, Name: "a", Payload: &tableA},
				},
			},
		}
		res, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)
		require.Len(t, res.ObjectGraph.Nodes, 1)
	})

	t.Run("rejects unsupported payloads", func(t *testing.T) {
		in := models.IntrospectionResult{
			KindsMap: map[models.ObjectKind][]models.Object{
				models.ObjectKindTable: {
					{ID: 1, Kind: models.ObjectKindTable, Name: "a", Payload: "not a table"},
				},
			},
		}
		_, err := graphbuilder.New().BuildGraph(ctx, in)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported table payload type")
	})

	t.Run("rejects nil table pointer payloads", func(t *testing.T) {
		var nilTable *models.Table
		in := models.IntrospectionResult{
			KindsMap: map[models.ObjectKind][]models.Object{
				models.ObjectKindTable: {
					{ID: 1, Kind: models.ObjectKindTable, Name: "a", Payload: nilTable},
				},
			},
		}
		_, err := graphbuilder.New().BuildGraph(ctx, in)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil table payload")
	})
}

// TestGraphBuilder_EdgeCases covers structurally tricky but common real-world
// shapes: self-referencing tables, multiple foreign keys to the same parent,
// dangling references, and the presence of non-table object kinds.
func TestGraphBuilder_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("self-referencing table forms a single-member cycle", func(t *testing.T) {
		// e.g. employee.manager_id -> employee.id
		const idEmp models.ObjectID = 7
		in := introspection(
			tableObject(idEmp, namedTable("employee", fk("employee", "fk_manager", "manager_id"))),
		)

		res, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)

		// The self-reference is a foreign-key edge from the table to itself.
		selfEdge, ok := findEdge(res.ObjectGraph.Edges[idEmp], idEmp)
		require.True(t, ok, "expected a self-edge")
		fkPayload, ok := selfEdge.Link.Payload.(models.ForeignKeyLinkPayload)
		require.True(t, ok)
		assert.Equal(t, []string{"manager_id"}, fkPayload.Columns)

		// A self-loop is a one-member cyclic SCC.
		require.Len(t, res.CondensedGraph.Nodes, 1)
		node := res.CondensedGraph.Nodes[res.ObjectToSCC[idEmp]]
		assert.Equal(t, []models.ObjectID{idEmp}, node.Members)
		require.NotNil(t, node.Cycles, "self-reference must be reported as a cycle")
		require.Len(t, node.Cycles.Cycles, 1)
		require.Len(t, node.Cycles.Cycles[0].Edges, 1)
		assert.Equal(t, idEmp, node.Cycles.Cycles[0].Edges[0].From)
		assert.Equal(t, idEmp, node.Cycles.Cycles[0].Edges[0].To)
	})

	t.Run("multiple foreign keys to the same parent", func(t *testing.T) {
		// e.g. message.sender_id -> user.id and message.receiver_id -> user.id
		const (
			idUser models.ObjectID = 1
			idMsg  models.ObjectID = 2
		)
		in := introspection(
			tableObject(idUser, namedTable("user")),
			tableObject(idMsg, namedTable("message",
				fk("user", "fk_sender", "sender_id"),
				fk("user", "fk_receiver", "receiver_id"),
			)),
		)

		res, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)

		// Both foreign keys appear as distinct parallel object edges.
		edges := res.ObjectGraph.Edges[idMsg]
		require.Len(t, edges, 2)
		var objConstraints []string
		for _, e := range edges {
			assert.Equal(t, idMsg, e.From)
			assert.Equal(t, idUser, e.To)
			objConstraints = append(objConstraints, e.Link.Payload.(models.ForeignKeyLinkPayload).ConstraintName)
		}
		assert.ElementsMatch(t, []string{"fk_sender", "fk_receiver"}, objConstraints)

		// The condensed edge collapses both into one SCCEdge carrying both links.
		cond := requireCondensedEdge(t, res.CondensedGraph,
			res.ObjectToSCC[idMsg], res.ObjectToSCC[idUser])
		require.Len(t, cond.Links, 2)
		var condConstraints []string
		for _, l := range cond.Links {
			condConstraints = append(condConstraints, l.Link.Payload.(models.ForeignKeyLinkPayload).ConstraintName)
		}
		assert.ElementsMatch(t, []string{"fk_sender", "fk_receiver"}, condConstraints)
	})

	t.Run("dangling reference returns an error", func(t *testing.T) {
		in := introspection(
			tableObject(1, namedTable("a", fk("ghost", "fk_ghost", "ghost_id"))),
		)
		_, err := graphbuilder.New().BuildGraph(ctx, in)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reference table not found")
	})

	t.Run("sparse and unordered object IDs", func(t *testing.T) {
		// ObjectID is global and may be sparse; positions in KindsMap are unrelated
		// to ID order. Use large, gapped, scrambled IDs and a cycle to ensure the
		// result is keyed purely by ObjectID with no dense-index assumption.
		const (
			idA models.ObjectID = 5
			idB models.ObjectID = 42
			idC models.ObjectID = 900900
		)
		// a <-> b (cycle), b -> c. Listed out of ID order on purpose.
		in := introspection(
			tableObject(idB, namedTable("b",
				fk("a", "fk_b_a", "a_id"),
				fk("c", "fk_b_c", "c_id"),
			)),
			tableObject(idC, namedTable("c")),
			tableObject(idA, namedTable("a", fk("b", "fk_a_b", "b_id"))),
		)

		res, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)

		// Nodes and the object->SCC map are keyed by the exact sparse IDs.
		assert.ElementsMatch(t,
			[]models.ObjectID{idA, idB, idC},
			slices.Collect(maps.Keys(res.ObjectGraph.Nodes)),
		)
		require.Len(t, res.ObjectToSCC, 3)

		// a and b collapse into one cyclic SCC; c is a separate singleton.
		sccAB := res.ObjectToSCC[idA]
		require.Equal(t, sccAB, res.ObjectToSCC[idB])
		require.NotEqual(t, sccAB, res.ObjectToSCC[idC])

		node := res.CondensedGraph.Nodes[sccAB]
		assert.Equal(t, []models.ObjectID{idA, idB}, node.Members) // sorted by ID
		require.NotNil(t, node.Cycles)
		require.NotEmpty(t, node.Cycles.Cycles)
		for _, e := range node.Cycles.Cycles[0].Edges {
			assert.Contains(t, []models.ObjectID{idA, idB}, e.From)
			assert.Contains(t, []models.ObjectID{idA, idB}, e.To)
		}

		// The cross-SCC link b -> c carries the sparse IDs verbatim.
		bridge := requireCondensedEdge(t, res.CondensedGraph, sccAB, res.ObjectToSCC[idC])
		require.Len(t, bridge.Links, 1)
		assert.Equal(t, idB, bridge.Links[0].From)
		assert.Equal(t, idC, bridge.Links[0].To)
	})

	t.Run("non-table object kinds are ignored", func(t *testing.T) {
		in := models.IntrospectionResult{
			KindsMap: map[models.ObjectKind][]models.Object{
				models.ObjectKindTable: {
					tableObject(1, namedTable("a")),
				},
				models.ObjectKindPostgresSequence: {
					{ID: 2, Kind: models.ObjectKindPostgresSequence, Name: "a_id_seq"},
				},
			},
		}
		res, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)
		require.Len(t, res.ObjectGraph.Nodes, 1)
		_, ok := res.ObjectGraph.Nodes[1]
		assert.True(t, ok, "only the table object participates in the graph")
		require.Len(t, res.ObjectToSCC, 1)
	})
}

func fk(refName, constraint, key string) models.Reference {
	return models.Reference{
		ReferencedSchema: "public",
		ReferencedName:   refName,
		ConstraintName:   constraint,
		Keys:             []string{key},
	}
}

func namedTable(name string, refs ...models.Reference) models.Table {
	return models.Table{
		Schema:     "public",
		Name:       name,
		PrimaryKey: []string{"id"},
		References: refs,
	}
}

func requireCondensedEdge(t *testing.T, cg models.CondensedGraph, from, to models.SCCID) models.SCCEdge {
	t.Helper()
	e, ok := findCondensedEdge(cg, from, to)
	require.Truef(t, ok, "expected condensed edge SCC(%d) -> SCC(%d)", from, to)
	return e
}

// cycleMembers returns the distinct object IDs touched by a cycle's edges.
func cycleMembers(c models.Cycle) []models.ObjectID {
	seen := map[models.ObjectID]struct{}{}
	var out []models.ObjectID
	for _, e := range c.Edges {
		for _, id := range []models.ObjectID{e.From, e.To} {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// sortedObjectIDs returns a sorted copy of the given object IDs.
func sortedObjectIDs(ids []models.ObjectID) []models.ObjectID {
	out := append([]models.ObjectID(nil), ids...)
	slices.Sort(out)
	return out
}

// TestGraphBuilder_Cycles groups the cyclic-graph cases: multiple components with
// independent cycles, a single SCC with overlapping cycle groups, and a chain of
// cycle groups linked to each other through shared tables.
func TestGraphBuilder_Cycles(t *testing.T) {
	ctx := context.Background()

	t.Run("multiple components with independent cycles", func(t *testing.T) {
		// Schema with three components and two real cycles:
		//
		//   table1_y -> table1_x -> {table1_a,table1_b,table1_c}
		//                                 -> {table2_a,table2_b} -> table3_a
		//
		// where {table1_a,table1_b,table1_c} is a 3-table cycle and
		// {table2_a,table2_b} is a 2-table cycle. The condensation must collapse
		// each cycle into one SCC and leave a clean DAG chain between components.

		// Object IDs are deliberately spread out (and not equal to slice positions)
		// to exercise the index<->ObjectID mapping.
		const (
			idY  models.ObjectID = 10 // table1_y
			idX  models.ObjectID = 20 // table1_x
			idA  models.ObjectID = 30 // table1_a
			idB  models.ObjectID = 40 // table1_b
			idC  models.ObjectID = 50 // table1_c
			id2A models.ObjectID = 60 // table2_a
			id2B models.ObjectID = 70 // table2_b
			id3A models.ObjectID = 80 // table3_a
		)

		in := introspection(
			tableObject(idY, namedTable("table1_y", fk("table1_x", "fk_table1_x", "table1_x_id"))),
			tableObject(idX, namedTable("table1_x", fk("table1_c", "fk_table1_c", "table1_c_id"))),
			tableObject(idA, namedTable("table1_a", fk("table1_c", "fk_table1_c", "table1_c_id"))),
			tableObject(idB, namedTable("table1_b", fk("table1_a", "fk_table1_a", "table1_a_id"))),
			tableObject(idC, namedTable("table1_c",
				fk("table1_b", "fk_table1_b", "table1_b_id"),
				fk("table2_a", "fk_table2_a", "table2_a_id"),
			)),
			tableObject(id2A, namedTable("table2_a", fk("table2_b", "fk_table2_b", "table2_b_id"))),
			tableObject(id2B, namedTable("table2_b",
				fk("table2_a", "fk_table2_a", "table2_a_id"),
				fk("table3_a", "fk_table3_a", "table3_a_id"),
			)),
			tableObject(id3A, namedTable("table3_a")),
		)

		res, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)

		// --- object graph: 8 nodes, 9 foreign-key edges ---
		require.Len(t, res.ObjectGraph.Nodes, 8)
		edgeCount := 0
		for _, es := range res.ObjectGraph.Edges {
			edgeCount += len(es)
		}
		assert.Equal(t, 9, edgeCount, "total foreign-key edges")
		require.Len(t, res.ObjectToSCC, 8)

		// --- condensation: 5 SCCs (2 cyclic + 3 singletons) ---
		require.Len(t, res.CondensedGraph.Nodes, 5)

		// Component 1 cycle: a, b, c collapse into one cyclic SCC.
		scc1 := res.ObjectToSCC[idA]
		require.Equal(t, scc1, res.ObjectToSCC[idB])
		require.Equal(t, scc1, res.ObjectToSCC[idC])
		node1 := res.CondensedGraph.Nodes[scc1]
		assert.Equal(t, []models.ObjectID{idA, idB, idC}, node1.Members) // sorted
		require.NotNil(t, node1.Cycles, "component 1 SCC must carry a cycle graph")
		require.Len(t, node1.Cycles.Cycles, 1, "component 1 has a single 3-table cycle")
		assert.Len(t, node1.Cycles.Cycles[0].Edges, 3)
		require.Len(t, node1.Cycles.Groups, 1)
		for _, g := range node1.Cycles.Groups {
			assert.ElementsMatch(t, []models.ObjectID{idA, idB, idC}, g.Members)
		}
		assert.Empty(t, node1.Cycles.GroupGraph, "a single cycle group has no inter-group edges")
		// Intra-SCC subgraph: 3 nodes and the 3 cycle edges.
		require.Len(t, node1.Subgraph.Nodes, 3)
		intra1 := 0
		for _, es := range node1.Subgraph.Edges {
			intra1 += len(es)
		}
		assert.Equal(t, 3, intra1)

		// Component 2 cycle: table2_a, table2_b collapse into one cyclic SCC.
		scc2 := res.ObjectToSCC[id2A]
		require.Equal(t, scc2, res.ObjectToSCC[id2B])
		node2 := res.CondensedGraph.Nodes[scc2]
		assert.Equal(t, []models.ObjectID{id2A, id2B}, node2.Members)
		require.NotNil(t, node2.Cycles)
		require.Len(t, node2.Cycles.Cycles, 1)
		assert.Len(t, node2.Cycles.Cycles[0].Edges, 2)

		// Singletons carry no cycle.
		for _, id := range []models.ObjectID{idX, idY, id3A} {
			n := res.CondensedGraph.Nodes[res.ObjectToSCC[id]]
			assert.Equal(t, []models.ObjectID{id}, n.Members)
			assert.Nil(t, n.Cycles)
			assert.Empty(t, n.Subgraph.Edges, "singleton SCC has no intra edges")
		}

		// --- condensed DAG chain: y -> x -> SCC1 -> SCC2 -> 3a ---
		sccY := res.ObjectToSCC[idY]
		sccX := res.ObjectToSCC[idX]
		scc3A := res.ObjectToSCC[id3A]

		requireCondensedEdge(t, res.CondensedGraph, sccY, sccX)
		requireCondensedEdge(t, res.CondensedGraph, sccX, scc1)
		requireCondensedEdge(t, res.CondensedGraph, scc2, scc3A)

		// The SCC1 -> SCC2 bridge is exactly table1_c -> table2_a.
		bridge := requireCondensedEdge(t, res.CondensedGraph, scc1, scc2)
		require.Len(t, bridge.Links, 1)
		assert.Equal(t, idC, bridge.Links[0].From)
		assert.Equal(t, id2A, bridge.Links[0].To)
		fkPayload, ok := bridge.Links[0].Link.Payload.(models.ForeignKeyLinkPayload)
		require.True(t, ok)
		assert.Equal(t, "fk_table2_a", fkPayload.ConstraintName)
		assert.Equal(t, []string{"table2_a_id"}, fkPayload.Columns)

		// The components are otherwise disjoint: no condensed edge skips the chain
		// (e.g. SCC1 must not connect directly to table3_a).
		_, ok = findCondensedEdge(res.CondensedGraph, scc1, scc3A)
		assert.False(t, ok, "SCC1 must reach table3_a only through SCC2")

		// Every object maps to exactly one of the five SCCs.
		distinct := map[models.SCCID]struct{}{}
		for _, scc := range res.ObjectToSCC {
			distinct[scc] = struct{}{}
		}
		assert.Len(t, distinct, 5)
	})

	t.Run("single scc with overlapping cycle groups", func(t *testing.T) {
		// Verifies the full CycleGraph — including the cycle group graph
		// (CycleGroupEdge / SharedObjects). The schema is a single SCC {a,b,c} with
		// two overlapping cycles that share the hub table b:
		//
		//   a <-> b   and   b <-> c
		const (
			idA models.ObjectID = 1
			idB models.ObjectID = 2
			idC models.ObjectID = 3
		)
		in := introspection(
			tableObject(idA, namedTable("a", fk("b", "fk_a_b", "b_id"))),
			tableObject(idB, namedTable("b",
				fk("a", "fk_b_a", "a_id"),
				fk("c", "fk_b_c", "c_id"),
			)),
			tableObject(idC, namedTable("c", fk("b", "fk_c_b", "b_id"))),
		)

		res, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)

		// All three tables collapse into a single cyclic SCC.
		require.Len(t, res.CondensedGraph.Nodes, 1)
		scc := res.ObjectToSCC[idA]
		require.Equal(t, scc, res.ObjectToSCC[idB])
		require.Equal(t, scc, res.ObjectToSCC[idC])
		node := res.CondensedGraph.Nodes[scc]
		require.NotNil(t, node.Cycles)
		cg := node.Cycles

		// Two 2-table cycles: {a,b} and {b,c}. Verify the actual edge contents, not
		// just the counts.
		require.Len(t, cg.Cycles, 2)
		cycleMemberSets := make([][]models.ObjectID, 0, len(cg.Cycles))
		for _, c := range cg.Cycles {
			assert.NotEmpty(t, c.ID, "every cycle has an ID")
			require.Len(t, c.Edges, 2)
			members := cycleMembers(c)
			require.Len(t, members, 2)
			// b participates in both cycles.
			assert.Contains(t, members, idB)
			cycleMemberSets = append(cycleMemberSets, members)
		}
		assert.ElementsMatch(t,
			[][]models.ObjectID{{idA, idB}, {idB, idC}},
			cycleMemberSets,
			"the two cycles are {a,b} and {b,c}",
		)

		// Two cycle groups, each referencing valid, in-range cycle indices.
		require.Len(t, cg.Groups, 2)
		for gid, g := range cg.Groups {
			assert.Equal(t, gid, g.ID, "group keyed by its own ID")
			require.NotEmpty(t, g.Cycles)
			for _, ci := range g.Cycles {
				require.GreaterOrEqual(t, int(ci), 0)
				require.Less(t, int(ci), len(cg.Cycles), "cycle index is in range")
			}
			assert.Contains(t, g.Members, idB, "hub b is in every group")
		}

		// The group graph is exercised here (empty elsewhere): groups are linked and
		// the shared object is the hub table b.
		require.NotEmpty(t, cg.GroupGraph, "overlapping cycle groups must be linked")
		var sawSharedHub bool
		for from, edges := range cg.GroupGraph {
			_, fromKnown := cg.Groups[from]
			assert.True(t, fromKnown, "group graph source is a known group")
			for _, e := range edges {
				assert.Equal(t, from, e.From, "edge keyed under its From group")
				_, toKnown := cg.Groups[e.To]
				assert.True(t, toKnown, "group graph target is a known group")
				assert.NotEqual(t, e.From, e.To, "no self-edge between identical groups")
				if slices.Contains(e.SharedObjects, idB) {
					sawSharedHub = true
				}
			}
		}
		assert.True(t, sawSharedHub, "cycle groups share the hub table b")
	})

	t.Run("chain of cycle groups linked through shared tables", func(t *testing.T) {
		// The hard case the subset engine must resolve: a single SCC containing
		// several cycle groups linked to each other through different shared tables.
		//
		// The schema is a chain of overlapping 2-cycles:
		//
		//   a <-> b <-> c <-> d
		//
		// which is one SCC {a,b,c,d} with three cycle groups {a,b}, {b,c}, {c,d}.
		// The adjacent groups are linked through the shared tables b and c; the
		// non-adjacent groups {a,b} and {c,d} share nothing and must not be linked.
		const (
			idA models.ObjectID = 1
			idB models.ObjectID = 2
			idC models.ObjectID = 3
			idD models.ObjectID = 4
		)
		in := introspection(
			tableObject(idA, namedTable("a", fk("b", "fk_a_b", "b_id"))),
			tableObject(idB, namedTable("b",
				fk("a", "fk_b_a", "a_id"),
				fk("c", "fk_b_c", "c_id"),
			)),
			tableObject(idC, namedTable("c",
				fk("b", "fk_c_b", "b_id"),
				fk("d", "fk_c_d", "d_id"),
			)),
			tableObject(idD, namedTable("d", fk("c", "fk_d_c", "c_id"))),
		)

		res, err := graphbuilder.New().BuildGraph(ctx, in)
		require.NoError(t, err)

		// One SCC holding all four tables.
		require.Len(t, res.CondensedGraph.Nodes, 1)
		scc := res.ObjectToSCC[idA]
		for _, id := range []models.ObjectID{idB, idC, idD} {
			require.Equalf(t, scc, res.ObjectToSCC[id], "table %d must be in the single SCC", id)
		}
		node := res.CondensedGraph.Nodes[scc]
		require.Equal(t, []models.ObjectID{idA, idB, idC, idD}, node.Members)
		require.NotNil(t, node.Cycles)
		cg := node.Cycles

		// Three overlapping 2-cycles: {a,b}, {b,c}, {c,d}.
		require.Len(t, cg.Cycles, 3)
		var cycleSets [][]models.ObjectID
		for _, c := range cg.Cycles {
			require.Len(t, c.Edges, 2)
			cycleSets = append(cycleSets, cycleMembers(c))
		}
		assert.ElementsMatch(t,
			[][]models.ObjectID{{idA, idB}, {idB, idC}, {idC, idD}},
			cycleSets,
		)

		// Three cycle groups, one per distinct vertex pair.
		require.Len(t, cg.Groups, 3)
		var groupSets [][]models.ObjectID
		for gid, g := range cg.Groups {
			assert.Equal(t, gid, g.ID)
			groupSets = append(groupSets, sortedObjectIDs(g.Members))
		}
		assert.ElementsMatch(t,
			[][]models.ObjectID{{idA, idB}, {idB, idC}, {idC, idD}},
			groupSets,
		)

		// The links between groups are the heart of this case: exactly two adjacency
		// links, joined through b and through c (the shared hubs), never through the
		// chain endpoints a/d, and never between the non-adjacent {a,b} and {c,d}.
		var sharedSets [][]models.ObjectID
		edgeCount := 0
		for from, edges := range cg.GroupGraph {
			_, fromKnown := cg.Groups[from]
			require.True(t, fromKnown, "group graph source must be a known group")
			for _, e := range edges {
				edgeCount++
				assert.Equal(t, from, e.From, "edge keyed under its From group")
				_, toKnown := cg.Groups[e.To]
				assert.True(t, toKnown, "group graph target must be a known group")
				assert.NotEqual(t, e.From, e.To, "no self-link")
				assert.NotContains(t, e.SharedObjects, idA, "chain endpoint a is never a shared hub")
				assert.NotContains(t, e.SharedObjects, idD, "chain endpoint d is never a shared hub")
				sharedSets = append(sharedSets, sortedObjectIDs(e.SharedObjects))
			}
		}
		require.Equal(t, 2, edgeCount, "two adjacency links: {a,b}-{b,c} and {b,c}-{c,d}")
		assert.ElementsMatch(t,
			[][]models.ObjectID{{idB}, {idC}},
			sharedSets,
			"adjacent groups are joined through b and c respectively",
		)
	})
}
