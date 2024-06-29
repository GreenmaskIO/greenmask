package subset

import (
	"context"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
)

var (
	foreignKeyColumnsQuery = `
		SELECT n.nspname            as             fk_table_schema,
			   fk_ref_table.relname as             fk_table_name,
			   array_agg(curr_table_attrs.attname) curr_table_columns
		FROM pg_catalog.pg_constraint curr_table_con
				 join pg_catalog.pg_class fk_ref_table on curr_table_con.confrelid = fk_ref_table.oid
				 join pg_catalog.pg_namespace n on fk_ref_table.relnamespace = n.oid
				 join pg_catalog.pg_attribute curr_table_attrs on curr_table_attrs.attrelid = curr_table_con.conrelid AND
																  curr_table_attrs.attnum = ANY (curr_table_con.conkey)
		WHERE curr_table_con.conrelid = $1
		  AND curr_table_con.contype = 'f'
		GROUP BY fk_table_schema, fk_table_name, curr_table_con.oid;
	`
)

// Graph - the graph representation of the DB tables. Is responsible for finding the cycles in the graph
// and searching subset Path for the tables
type Graph struct {
	// tables - the tables that are in the scope. They were previously fetched from the DB by RuntimeContext
	tables []*entries.Table
	// graph - the oriented graph representation of the DB tables
	graph [][]*Edge
	// cycledVertexes - it shows last vertex before cycled edge
	cycledVertexes map[int][]*Edge
	// cycles - the cycles in the graph with topological order
	cycles [][]int
	// Paths - the subset Paths for the tables. The key is the vertex index in the graph and the value is the path for
	// creating the subset query
	Paths map[int]*Path
	edges []*Edge
}

// NewGraph creates a new graph based on the provided tables by finding the references in DB between them
func NewGraph(ctx context.Context, tx pgx.Tx, tables []*entries.Table) (*Graph, error) {
	orientedGraph := make([][]*Edge, len(tables))
	edges := make([]*Edge, 0)

	var edgeIdSequence int
	for idx, table := range tables {
		refs, err := getReferences(ctx, tx, table.Oid)
		if err != nil {
			return nil, fmt.Errorf("error getting references: %w", err)
		}
		for _, ref := range refs {
			foreignTableIdx := slices.IndexFunc(tables, func(t *entries.Table) bool {
				return t.Name == ref.Name && t.Schema == ref.Schema
			})

			if foreignTableIdx == -1 {
				log.Debug().
					Str("Schema", ref.Schema).
					Str("Table", ref.Name).
					Msg("unable to find foreign table: it might be excluded from the dump")
				continue
			}
			edge := NewEdge(
				edgeIdSequence,
				foreignTableIdx,
				NewTableLink(idx, table, ref.ReferencedKeys),
				NewTableLink(foreignTableIdx, tables[foreignTableIdx], tables[foreignTableIdx].PrimaryKey),
			)
			orientedGraph[idx] = append(
				orientedGraph[idx],
				edge,
			)
			edges = append(edges, edge)

			edgeIdSequence++
		}
	}
	return &Graph{
		tables:         tables,
		graph:          orientedGraph,
		cycledVertexes: make(map[int][]*Edge),
		Paths:          make(map[int]*Path),
		edges:          edges,
	}, nil
}

// findCycles - finds all cycles in the graph
func (g *Graph) findCycles() {
	visited := make([]int, len(g.graph))
	from := make([]int, len(g.graph))
	for idx := range from {
		from[idx] = emptyFromValue
	}
	for v := range g.graph {
		if visited[v] == vertexIsNotVisited {
			g.findAllCyclesDfs(v, visited, from)
		}
	}
	g.debugCycles()
}

// debugCycles - debugs the cycles in the graph
func (g *Graph) debugCycles() {
	if len(g.cycles) == 0 {
		return
	}

	for _, foundCycle := range g.cycles {
		var cycle []string
		for _, v := range foundCycle {
			cycle = append(cycle, fmt.Sprintf("%s.%s", g.tables[v].Schema, g.tables[v].Name))
		}
		cycle = append(cycle, fmt.Sprintf("%s.%s", g.tables[foundCycle[0]].Schema, g.tables[foundCycle[0]].Name))
		if slices.ContainsFunc(foundCycle, func(i int) bool {
			return len(g.tables[i].SubsetConds) > 0
		}) {
			log.Warn().Strs("cycle", cycle).Msg("cycle detected")
			panic("IMPLEMENT ME: cycle detected: implement cycles resolution")
		}
	}

}

// findAllCyclesDfs - the basic DFS algorithm adapted to find all cycles in the graph and collect the cycle vertices
func (g *Graph) findAllCyclesDfs(v int, visited []int, from []int) {
	visited[v] = vertexIsVisitedAndPrecessing
	for _, to := range g.graph[v] {
		if visited[to.Idx] == vertexIsNotVisited {
			from[to.Idx] = v
			g.findAllCyclesDfs(to.Idx, visited, from)
		} else if visited[to.Idx] == vertexIsVisitedAndPrecessing {
			from[to.Idx] = v
			g.cycles = append(g.cycles, g.getCycle(from, to.Idx))
		}
	}
	visited[v] = vertexIsVisitedAndCompleted
}

// getCycle returns the cycle in the graph provided based on the "from" slice
func (g *Graph) getCycle(from []int, lastVertex int) []int {
	var cycle []int
	for v := from[lastVertex]; v != lastVertex; v = from[v] {
		cycle = append(cycle, v)
	}
	cycle = append(cycle, lastVertex)
	slices.Reverse(cycle)
	return cycle
}

// findSubsetVertexes - finds the subset vertexes in the graph
func (g *Graph) findSubsetVertexes() {
	for v := range g.graph {
		path := NewPath(v)
		visited := make([]int, len(g.graph))
		var from, fullFrom []*Edge
		if len(g.tables[v].SubsetConds) > 0 {
			path.AddVertex(v)
		}
		g.subsetDfs(path, v, &fullFrom, &from, visited, rootScopeId)

		if path.Len() > 0 {
			g.Paths[v] = path
		}
	}
}

func (g *Graph) subsetDfs(path *Path, v int, fullFrom, from *[]*Edge, visited []int, scopeId int) {
	visited[v] = vertexIsVisitedAndPrecessing
	for _, to := range g.graph[v] {
		*fullFrom = append(*fullFrom, to)
		*from = append(*from, to)
		currentScopeId := scopeId
		if visited[to.Idx] == vertexIsNotVisited {
			if len(g.tables[to.Idx].SubsetConds) > 0 {
				for _, e := range *from {
					currentScopeId = path.AddEdge(e, currentScopeId)
				}
				*from = (*from)[:0]
			}
			g.subsetDfs(path, to.Idx, fullFrom, from, visited, currentScopeId)
		} else if visited[to.Idx] == vertexIsVisitedAndPrecessing {
			// if the vertex is visited and processing, it means that we found a cycle, and we need to mark the edge
			// as cycled and collect the cycle. This data will be used later for cycle resolution
			log.Debug().Msg("cycle detected")
			g.cycledVertexes[to.Id] = slices.Clone(*fullFrom)
		}
		*fullFrom = (*fullFrom)[:len(*fullFrom)-1]
		if len(*from) > 0 {
			*from = (*from)[:len(*from)-1]
		}
	}
	visited[v] = vertexIsNotVisited
}

func getReferences(ctx context.Context, tx pgx.Tx, tableOid toolkit.Oid) ([]*toolkit.Reference, error) {
	var refs []*toolkit.Reference
	rows, err := tx.Query(ctx, foreignKeyColumnsQuery, tableOid)
	if err != nil {
		return nil, fmt.Errorf("error executing ForeignKeyColumnsQuery: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		ref := &toolkit.Reference{}
		if err = rows.Scan(&ref.Schema, &ref.Name, &ref.ReferencedKeys); err != nil {
			return nil, fmt.Errorf("error scanning ForeignKeyColumnsQuery: %w", err)
		}
		refs = append(refs, ref)
	}
	return refs, nil
}
