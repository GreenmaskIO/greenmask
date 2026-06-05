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

// Package graphbuilder builds the dependency graph for a dump from an
// introspection result. It is the reusable home of the "graph" pipeline stage
// (interfaces.DependencyGraphBuilder).
//
// It produces all parts of the dependency model in a single pass:
//
//   - the object graph (the directed DAG-or-not graph of objects and their
//     foreign-key links),
//   - the condensed graph (the DAG of strongly connected components), and
//   - per-SCC cycle graphs (cycles, cycle groups and the graph between them).
//
// The heavy graph algorithms (reference graph construction, Kosaraju SCC
// condensation and cycle detection) are reused from the engine-agnostic
// pkg/common/subset/{tablegraph,condensationgraph,cyclesgraph} packages; this
// package orchestrates them and translates their position-based output into the
// ObjectID/SCCID-based models.DependencyGraphResult consumed by the pipeline.
//
// ObjectID is a global identifier (unique across object kinds) and may be sparse.
// The algorithm packages, however, identify each vertex by a dense slice position
// (0..n-1). The translator below keeps these two worlds apart: vertex data is
// addressed by position only through a single position->ObjectID bridge, and
// everything the caller sees is keyed by ObjectID.
package graphbuilder

import (
	"context"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/subset/condensationgraph"
	"github.com/greenmaskio/greenmask/pkg/common/subset/tablegraph"
)

var _ interfaces.DependencyGraphBuilder = (*GraphBuilder)(nil)

// GraphBuilder is the engine-agnostic implementation of
// interfaces.DependencyGraphBuilder.
type GraphBuilder struct{}

// New creates a GraphBuilder.
func New() *GraphBuilder {
	return &GraphBuilder{}
}

// BuildGraph builds the dependency graph from the introspection result.
//
// Only table objects participate in the graph (foreign keys are the edges).
// Other object kinds, if present, are ignored here.
func (b *GraphBuilder) BuildGraph(
	_ context.Context,
	introspection commonmodels.IntrospectionResult,
) (commonmodels.DependencyGraphResult, error) {
	tableObjects := introspection.KindsMap[commonmodels.ObjectKindTable]

	// tables is positional (index == dense graph vertex position) and is only used
	// to feed the algorithm packages. Everything the translator exposes is keyed by
	// the global ObjectID instead.
	tables := make([]commonmodels.Table, len(tableObjects))
	tr := &translator{
		objectIDByPos:  make([]commonmodels.ObjectID, len(tableObjects)),
		objectIDByName: make(map[string]commonmodels.ObjectID, len(tableObjects)),
		nodes:          make(map[commonmodels.ObjectID]commonmodels.ObjectNode, len(tableObjects)),
		refsByID:       make(map[commonmodels.ObjectID][]commonmodels.Reference, len(tableObjects)),
	}
	for i, obj := range tableObjects {
		tbl, err := tableFromObject(obj)
		if err != nil {
			return commonmodels.DependencyGraphResult{}, err
		}
		tables[i] = tbl
		tr.objectIDByPos[i] = obj.ID
		tr.objectIDByName[tbl.FullTableName()] = obj.ID
		tr.nodes[obj.ID] = commonmodels.ObjectNode{
			ID:      obj.ID,
			Kind:    obj.Kind,
			Name:    obj.Name,
			Payload: obj.Payload,
		}
		tr.refsByID[obj.ID] = tbl.References
	}

	result := commonmodels.DependencyGraphResult{
		ObjectGraph: commonmodels.ObjectGraph{
			Nodes: map[commonmodels.ObjectID]commonmodels.ObjectNode{},
			Edges: map[commonmodels.ObjectID][]commonmodels.ObjectEdge{},
		},
		CondensedGraph: commonmodels.CondensedGraph{
			Nodes: map[commonmodels.SCCID]commonmodels.SCCNode{},
			Edges: map[commonmodels.SCCID][]commonmodels.SCCEdge{},
		},
		ObjectToSCC: map[commonmodels.ObjectID]commonmodels.SCCID{},
	}
	if len(tables) == 0 {
		return result, nil
	}

	tg, err := tablegraph.NewGraph(tables)
	if err != nil {
		return commonmodels.DependencyGraphResult{}, fmt.Errorf("build table graph: %w", err)
	}
	cg := condensationgraph.NewGraph(tg)

	result.ObjectGraph = tr.buildObjectGraph(tg)
	result.CondensedGraph, result.ObjectToSCC = tr.buildCondensedGraph(cg)
	return result, nil
}

// translator converts the position-based subgraphs produced by the algorithm
// packages into the ObjectID-based result model.
//
// Vertexes are addressed by dense graph position only where the algorithm
// packages hand back a position; objectIDByPos is the single crossover point to
// the global ObjectID. All other state is keyed by ObjectID.
type translator struct {
	// objectIDByPos maps a dense graph vertex position (0..n-1) to its ObjectID.
	objectIDByPos []commonmodels.ObjectID
	// objectIDByName maps a table's fully-qualified name to its ObjectID. It
	// resolves the shared vertexes that the cycle graph reports as tables rather
	// than positions.
	objectIDByName map[string]commonmodels.ObjectID
	// nodes holds every table object node keyed by its ObjectID.
	nodes map[commonmodels.ObjectID]commonmodels.ObjectNode
	// refsByID holds each table's foreign-key references keyed by its ObjectID,
	// used to recover constraint names for edges.
	refsByID map[commonmodels.ObjectID][]commonmodels.Reference
}

// idAt returns the ObjectID of the vertex at the given dense graph position.
func (t *translator) idAt(pos int) commonmodels.ObjectID {
	return t.objectIDByPos[pos]
}

// nodeAt returns the object node of the vertex at the given dense graph position.
func (t *translator) nodeAt(pos int) commonmodels.ObjectNode {
	return t.nodes[t.objectIDByPos[pos]]
}

// tableFromObject extracts a commonmodels.Table from an introspection object's
// payload. It accepts a common table directly or any engine-specific payload that
// can convert itself via ToCommonTable (e.g. *mysql.Table, *postgres.Table).
func tableFromObject(obj commonmodels.Object) (commonmodels.Table, error) {
	switch p := obj.Payload.(type) {
	case commonmodels.Table:
		return p, nil
	case *commonmodels.Table:
		if p == nil {
			return commonmodels.Table{}, fmt.Errorf("object %d (%q): nil table payload", obj.ID, obj.Name)
		}
		return *p, nil
	case interface{ ToCommonTable() commonmodels.Table }:
		return p.ToCommonTable(), nil
	default:
		return commonmodels.Table{}, fmt.Errorf(
			"object %d (%q): unsupported table payload type %T", obj.ID, obj.Name, obj.Payload,
		)
	}
}

// objectEdge converts a table-graph foreign-key edge into the result model.
//
// In the table graph the edge points from the referencing (child) table to the
// referenced (parent) table; From carries the foreign-key columns and To carries
// the referenced primary-key columns.
func (t *translator) objectEdge(e tablegraph.Edge) commonmodels.ObjectEdge {
	fromID := t.idAt(e.From().TableID())
	toID := t.idAt(e.To().TableID())

	fromCols := keyNames(e.From().Keys())
	toCols := keyNames(e.To().Keys())

	return commonmodels.ObjectEdge{
		From: fromID,
		To:   toID,
		Link: commonmodels.ObjectLink{
			Kind: commonmodels.ObjectLinkKindForeignKey,
			From: commonmodels.ObjectLinkEndpoint{ObjectID: fromID, Fields: fieldRefs(e.From().Keys())},
			To:   commonmodels.ObjectLinkEndpoint{ObjectID: toID, Fields: fieldRefs(e.To().Keys())},
			Payload: commonmodels.ForeignKeyLinkPayload{
				ConstraintName: t.constraintName(fromID, e.To().Table(), fromCols),
				Columns:        fromCols,
				RefColumns:     toCols,
				IsNullable:     e.IsNullable(),
			},
		},
	}
}

// constraintName recovers the foreign-key constraint name for an edge by matching
// the referencing object's references against the referenced table and key
// columns. The table graph does not carry the constraint name on the edge, so
// OnDelete and OnUpdate remain unset (the introspection Reference model does not
// expose them).
func (t *translator) constraintName(fromID commonmodels.ObjectID, refTable commonmodels.Table, fromCols []string) string {
	for _, ref := range t.refsByID[fromID] {
		if ref.ReferencedSchema == refTable.Schema &&
			ref.ReferencedName == refTable.Name &&
			slices.Equal(ref.Keys, fromCols) {
			return ref.ConstraintName
		}
	}
	return ""
}

// keyNames returns the column name (or expression) for each key.
func keyNames(keys []tablegraph.Key) []string {
	res := make([]string, 0, len(keys))
	for _, k := range keys {
		if k.Expression != "" {
			res = append(res, k.Expression)
		} else {
			res = append(res, k.Name)
		}
	}
	return res
}

// fieldRefs converts join keys into typed object field references.
func fieldRefs(keys []tablegraph.Key) []commonmodels.ObjectFieldRef {
	res := make([]commonmodels.ObjectFieldRef, 0, len(keys))
	for _, k := range keys {
		if k.Expression != "" {
			res = append(res, commonmodels.ObjectFieldRef{
				Kind:  commonmodels.FieldRefKindExpression,
				Value: k.Expression,
			})
			continue
		}
		res = append(res, commonmodels.ObjectFieldRef{
			Kind:  commonmodels.FieldRefKindColumn,
			Value: k.Name,
		})
	}
	return res
}
