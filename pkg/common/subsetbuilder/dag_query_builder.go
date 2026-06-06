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

package subsetbuilder

import (
	"cmp"
	"fmt"
	"regexp"
	"slices"

	"github.com/huandu/go-sqlbuilder"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/subset"
)

// dagQueryBuilder builds a SELECT query for a DAG (acyclic) SCC — one whose
// root has exactly one member table.  It mirrors dagQueryBuilder in
// pkg/common/subset/dag_query_builder.go but operates on DependencyGraphResult
// model types and also supports DialectPostgres (the original panicked there).
type dagQueryBuilder struct {
	sg          *sccSubgraph
	dg          commonmodels.DependencyGraphResult
	subsetConds map[commonmodels.ObjectID][]string
	dialect     subset.Dialect
}

func newDAGQueryBuilder(
	sg *sccSubgraph,
	dg commonmodels.DependencyGraphResult,
	subsetConds map[commonmodels.ObjectID][]string,
	dialect subset.Dialect,
) *dagQueryBuilder {
	return &dagQueryBuilder{sg: sg, dg: dg, subsetConds: subsetConds, dialect: dialect}
}

// build constructs the SELECT query for the root table of the sub-graph.
// Returns a single-entry map {rootObjectID: query}.
func (b *dagQueryBuilder) build() (map[commonmodels.ObjectID]string, error) {
	rootNode := b.dg.CondensedGraph.Nodes[b.sg.rootSCCID]
	rootOID := rootNode.Members[0]
	rootTable, err := tableFromPayload(b.dg.ObjectGraph.Nodes[rootOID].Payload)
	if err != nil {
		return nil, fmt.Errorf("get root table for SCC %d: %w", b.sg.rootSCCID, err)
	}

	aliasMap := b.makeAliases()
	tableAliasMap := make(map[commonmodels.ObjectID]string)

	flavor := dialectFlavor(b.dialect)
	rootName := tableName(b.dialect, rootTable, rootOID, tableAliasMap)
	sb := flavor.NewSelectBuilder().Select(rootName + ".*").From(rootName)

	if conds, ok := b.subsetConds[rootOID]; ok {
		sb.Where(sb.And(conds...))
	}

	b.buildQueryDFS(b.sg.rootSCCID, aliasMap, tableAliasMap, sb)

	return map[commonmodels.ObjectID]string{rootOID: sb.String()}, nil
}

// buildQueryDFS adds JOINs and WHERE conditions by DFS over the sub-graph.
func (b *dagQueryBuilder) buildQueryDFS(
	v commonmodels.SCCID,
	aliasMap map[edgeKey]string,
	tableAliasMap map[commonmodels.ObjectID]string,
	sb *sqlbuilder.SelectBuilder,
) {
	edges := b.sg.graph[v]
	sorted := make([]commonmodels.SCCEdge, len(edges))
	copy(sorted, edges)
	slices.SortFunc(sorted, func(a, c commonmodels.SCCEdge) int { return cmp.Compare(a.To, c.To) })

	for _, sccEdge := range sorted {
		for linkIdx, objEdge := range sccEdge.Links {
			fkp, ok := objEdge.Link.Payload.(commonmodels.ForeignKeyLinkPayload)
			if !ok {
				continue
			}

			fromOID := objEdge.From
			toOID := objEdge.To
			fromTable, err := tableFromPayload(b.dg.ObjectGraph.Nodes[fromOID].Payload)
			if err != nil {
				continue
			}
			toTable, err := tableFromPayload(b.dg.ObjectGraph.Nodes[toOID].Payload)
			if err != nil {
				continue
			}

			key := edgeKey{fromSCC: v, toSCC: sccEdge.To, linkIdx: linkIdx}
			alias, hasAlias := aliasMap[key]
			if hasAlias {
				tableAliasMap[toOID] = alias
			}

			leftCols := fieldNames(b.dialect, fromTable, fromOID, objEdge.Link.From.Fields, tableAliasMap)
			rightName := tableName(b.dialect, toTable, toOID, tableAliasMap)
			rightCols := fieldNames(b.dialect, toTable, toOID, objEdge.Link.To.Fields, tableAliasMap)

			joinMode := sqlbuilder.InnerJoin
			if fkp.IsNullable {
				joinMode = sqlbuilder.LeftJoin
			}
			joinCond := joinCondition(sb, leftCols, rightCols)
			sb.JoinWithOption(joinMode, rightName, joinCond)

			if conds, ok := b.subsetConds[toOID]; ok {
				revisedConds := conds
				if alias != "" {
					revisedConds = conditionsWithAlias(b.dialect, toTable, alias, conds)
				}
				addSubsetCondition(sb, fkp.IsNullable, revisedConds, leftCols)
			}

			if hasAlias {
				delete(tableAliasMap, toOID)
			}
		}
		b.buildQueryDFS(sccEdge.To, aliasMap, tableAliasMap, sb)
	}
}

// makeAliases assigns SQL aliases to ObjectEdges whose destination table
// appears more than once in the sub-graph joins.
// Mirrors makeTableAliasesForDAG in pkg/common/subset/dag_query_builder.go.
func (b *dagQueryBuilder) makeAliases() map[edgeKey]string {
	type edgeEntry struct {
		key edgeKey
		tbl commonmodels.Table
	}
	byDest := make(map[commonmodels.ObjectID][]edgeEntry)

	for _, fromSCC := range sortedSCCIDsFromMap(b.sg.graph) {
		for _, sccEdge := range b.sg.graph[fromSCC] {
			for linkIdx, objEdge := range sccEdge.Links {
				toOID := objEdge.Link.To.ObjectID
				tbl, err := tableFromPayload(b.dg.ObjectGraph.Nodes[toOID].Payload)
				if err != nil {
					continue
				}
				byDest[toOID] = append(byDest[toOID], edgeEntry{
					key: edgeKey{fromSCC: fromSCC, toSCC: sccEdge.To, linkIdx: linkIdx},
					tbl: tbl,
				})
			}
		}
	}

	aliasMap := make(map[edgeKey]string)
	for _, entries := range byDest {
		if len(entries) < 2 {
			continue
		}
		for i, e := range entries {
			aliasMap[e.key] = tableAlias(e.tbl, i)
		}
	}
	return aliasMap
}

// ── edgeKey ───────────────────────────────────────────────────────────────────

// edgeKey uniquely identifies one ObjectEdge within a sub-graph for alias
// assignment: (fromSCC, toSCC, index within SCCEdge.Links).
type edgeKey struct {
	fromSCC commonmodels.SCCID
	toSCC   commonmodels.SCCID
	linkIdx int
}

// ── SQL helpers ───────────────────────────────────────────────────────────────

func dialectFlavor(d subset.Dialect) sqlbuilder.Flavor {
	switch d {
	case subset.DialectMySQL:
		return sqlbuilder.MySQL
	case subset.DialectPostgres:
		return sqlbuilder.PostgreSQL
	default:
		panic(fmt.Sprintf("unsupported dialect: %s", d))
	}
}

func tableName(d subset.Dialect, t commonmodels.Table, oid commonmodels.ObjectID, aliasMap map[commonmodels.ObjectID]string) string {
	if alias, ok := aliasMap[oid]; ok {
		switch d {
		case subset.DialectPostgres:
			return fmt.Sprintf(`"%s"`, alias)
		case subset.DialectMySQL:
			return fmt.Sprintf("`%s`", alias)
		}
	}
	switch d {
	case subset.DialectPostgres:
		return fmt.Sprintf(`"%s"."%s"`, t.Schema, t.Name)
	case subset.DialectMySQL:
		return fmt.Sprintf("`%s`.`%s`", t.Schema, t.Name)
	}
	panic(fmt.Sprintf("unsupported dialect: %s", d))
}

func columnName(d subset.Dialect, t commonmodels.Table, oid commonmodels.ObjectID, col string, aliasMap map[commonmodels.ObjectID]string) string {
	if alias, ok := aliasMap[oid]; ok {
		switch d {
		case subset.DialectPostgres:
			return fmt.Sprintf(`"%s"."%s"`, alias, col)
		case subset.DialectMySQL:
			return fmt.Sprintf("`%s`.`%s`", alias, col)
		}
	}
	switch d {
	case subset.DialectPostgres:
		return fmt.Sprintf(`"%s"."%s"."%s"`, t.Schema, t.Name, col)
	case subset.DialectMySQL:
		return fmt.Sprintf("`%s`.`%s`.`%s`", t.Schema, t.Name, col)
	}
	panic(fmt.Sprintf("unsupported dialect: %s", d))
}

func fieldNames(
	d subset.Dialect,
	t commonmodels.Table,
	oid commonmodels.ObjectID,
	fields []commonmodels.ObjectFieldRef,
	aliasMap map[commonmodels.ObjectID]string,
) []string {
	res := make([]string, 0, len(fields))
	for _, f := range fields {
		if f.Kind == commonmodels.FieldRefKindExpression {
			panic("expression field refs are not yet supported in subsetbuilder")
		}
		res = append(res, columnName(d, t, oid, f.Value, aliasMap))
	}
	return res
}

func joinCondition(sb *sqlbuilder.SelectBuilder, leftCols, rightCols []string) string {
	parts := make([]string, len(leftCols))
	for i := range leftCols {
		parts[i] = sb.Equal(leftCols[i], sqlbuilder.Raw(rightCols[i]))
	}
	return sb.And(parts...)
}

// addSubsetCondition mirrors addSubsetConditions in pkg/common/subset/common.go.
func addSubsetCondition(
	sb *sqlbuilder.SelectBuilder,
	isNullable bool,
	conds []string,
	leftCols []string,
) {
	if !isNullable {
		sb.Where(sb.And(conds...))
		return
	}
	nullChecks := make([]string, len(leftCols))
	for i, c := range leftCols {
		nullChecks[i] = sb.IsNull(c)
	}
	sb.Where(sb.Or(sb.Or(nullChecks...), sb.And(conds...)))
}

// conditionsWithAlias rewrites subset conditions replacing schema.table
// references with the assigned alias.
// Mirrors getSubsetConditionsWithTableAlias in pkg/common/subset/common.go.
func conditionsWithAlias(d subset.Dialect, t commonmodels.Table, alias string, conds []string) []string {
	var ptrn string
	switch d {
	case subset.DialectMySQL:
		ptrn = fmt.Sprintf("`?%s`?.`?%s`?", t.Schema, t.Name)
	default:
		ptrn = fmt.Sprintf(`"?%s"?."?%s"?`, t.Schema, t.Name)
	}
	re := regexp.MustCompile(ptrn)
	out := make([]string, len(conds))
	for i, c := range conds {
		out[i] = re.ReplaceAllString(c, alias)
	}
	return out
}

// tableAlias generates a deterministic alias for a table occurrence.
// Mirrors getTableAlias in pkg/common/subset/common.go.
func tableAlias(t commonmodels.Table, seq int) string {
	return fmt.Sprintf("%s_%s__%d", t.Schema, t.Name, seq)
}
