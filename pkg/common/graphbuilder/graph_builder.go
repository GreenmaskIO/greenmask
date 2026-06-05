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
// package orchestrates them and translates their index-based output into the
// ObjectID/SCCID-based models.DependencyGraphResult consumed by the pipeline.
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

	// tables drives the underlying graph algorithms, which key everything on the
	// table's slice position. objectIDByIndex maps that slice position back to the
	// introspection ObjectID so the result is expressed in ObjectIDs.
	tables := make([]commonmodels.Table, len(tableObjects))
	objectIDByIndex := make([]commonmodels.ObjectID, len(tableObjects))
	for i, obj := range tableObjects {
		t, err := tableFromObject(obj)
		if err != nil {
			return commonmodels.DependencyGraphResult{}, err
		}
		// Reindex so the table's ID equals its slice position: the graph packages
		// identify vertexes by slice index and surface tables (not indexes) back,
		// so this keeps the index<->ObjectID mapping unambiguous.
		t.ID = i
		tables[i] = t
		objectIDByIndex[i] = obj.ID
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

	t := &translator{
		tableObjects:    tableObjects,
		tables:          tables,
		objectIDByIndex: objectIDByIndex,
	}

	result.ObjectGraph = t.buildObjectGraph(tg)
	result.CondensedGraph, result.ObjectToSCC = t.buildCondensedGraph(cg)
	return result, nil
}

// translator carries the index<->ObjectID mapping and source metadata used while
// converting the index-based subgraphs into the ObjectID-based result model.
type translator struct {
	tableObjects    []commonmodels.Object
	tables          []commonmodels.Table
	objectIDByIndex []commonmodels.ObjectID
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

// objectNode builds the node for the table at the given slice index.
func (t *translator) objectNode(idx int) commonmodels.ObjectNode {
	obj := t.tableObjects[idx]
	return commonmodels.ObjectNode{
		ID:      t.objectIDByIndex[idx],
		Kind:    obj.Kind,
		Name:    obj.Name,
		Payload: obj.Payload,
	}
}

// objectEdge converts a table-graph foreign-key edge into the result model.
//
// In the table graph the edge points from the referencing (child) table to the
// referenced (parent) table; From carries the foreign-key columns and To carries
// the referenced primary-key columns.
func (t *translator) objectEdge(e tablegraph.Edge) commonmodels.ObjectEdge {
	fromIdx := e.From().TableID()
	toIdx := e.To().TableID()
	fromID := t.objectIDByIndex[fromIdx]
	toID := t.objectIDByIndex[toIdx]

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
				ConstraintName: t.constraintName(fromIdx, e.To().Table(), fromCols),
				Columns:        fromCols,
				RefColumns:     toCols,
				IsNullable:     e.IsNullable(),
			},
		},
	}
}

// constraintName recovers the foreign-key constraint name for an edge by matching
// the referencing table's references against the referenced table and key columns.
// The table graph does not carry the constraint name on the edge, so OnDelete and
// OnUpdate remain unset (the introspection Reference model does not expose them).
func (t *translator) constraintName(fromIdx int, refTable commonmodels.Table, fromCols []string) string {
	for _, ref := range t.tables[fromIdx].References {
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
