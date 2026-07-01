package core

type ObjectNode struct {
	ID      ObjectID
	Kind    ObjectKind
	Name    string
	Payload any
}

type ObjectEdge struct {
	From ObjectID
	To   ObjectID
	Link ObjectLink
}

type ObjectLinkKind string

const (
	ObjectLinkKindForeignKey ObjectLinkKind = "foreign_key"
	ObjectLinkKindReference  ObjectLinkKind = "reference"
	ObjectLinkKindOwnership  ObjectLinkKind = "ownership"
	ObjectLinkKindDependency ObjectLinkKind = "dependency"
)

type FieldRefKind string

const (
	FieldRefKindColumn     FieldRefKind = "column"
	FieldRefKindExpression FieldRefKind = "expression"
)

type ObjectFieldRef struct {
	Kind  FieldRefKind
	Value string
}

type ObjectLinkEndpoint struct {
	ObjectID ObjectID
	// Optional. For table-like objects.
	Fields []ObjectFieldRef
}

type ForeignKeyLinkPayload struct {
	ConstraintName string
	Columns        []string
	RefColumns     []string
	OnDelete       string
	OnUpdate       string
	IsNullable     bool
}

type ObjectLink struct {
	Kind    ObjectLinkKind
	From    ObjectLinkEndpoint
	To      ObjectLinkEndpoint
	Payload any
}

type ObjectGraph struct {
	Nodes map[ObjectID]ObjectNode
	Edges map[ObjectID][]ObjectEdge
}

// HasCycles reports whether the graph contains any cycle (including self-loops).
// It uses an iterative DFS with a white/gray/black colouring to detect back-edges.
func (g ObjectGraph) HasCycles() bool {
	return g.hasCyclesDFS(nil)
}

// HasCyclesFor reports whether a cycle exists among the given subset of object IDs.
// Only edges between nodes present in ids are considered.
func (g ObjectGraph) HasCyclesFor(ids map[ObjectID]struct{}) bool {
	return g.hasCyclesDFS(ids)
}

// hasCyclesDFS performs an iterative DFS cycle check.
// When ids is non-nil only the nodes and edges within ids are considered;
// when ids is nil the full graph is traversed.
func (g ObjectGraph) hasCyclesDFS(ids map[ObjectID]struct{}) bool {
	const (
		white = 0 // unvisited
		gray  = 1 // on current DFS path
		black = 2 // fully processed
	)
	color := make(map[ObjectID]int, len(g.Nodes))

	type frame struct {
		id      ObjectID
		edgeIdx int
	}

	roots := g.Nodes
	if ids != nil {
		// Only start DFS from in-scope nodes.
		roots = make(map[ObjectID]ObjectNode, len(ids))
		for id := range ids {
			if node, ok := g.Nodes[id]; ok {
				roots[id] = node
			}
		}
	}

	for startID := range roots {
		if color[startID] != white {
			continue
		}
		stack := []frame{{id: startID}}
		color[startID] = gray
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			edges := g.Edges[top.id]
			advanced := false
			for top.edgeIdx < len(edges) {
				next := edges[top.edgeIdx].To
				top.edgeIdx++
				// When scoped, skip edges to out-of-scope nodes.
				if ids != nil {
					if _, ok := ids[next]; !ok {
						continue
					}
				}
				switch color[next] {
				case gray:
					return true // back-edge → cycle
				case white:
					color[next] = gray
					stack = append(stack, frame{id: next})
					advanced = true
				}
				if advanced {
					break
				}
			}
			if !advanced {
				color[top.id] = black
				stack = stack[:len(stack)-1]
			}
		}
	}
	return false
}

type SCCID int
type SCCEdge struct {
	From SCCID
	To   SCCID
	// Edges from object graph that caused this SCC-level edge.
	Links []ObjectEdge
}
type SCCNode struct {
	ID       SCCID
	Members  []ObjectID
	Subgraph ObjectGraph
	// Optional. Present only when the SCC contains real cycles
	// that require special subset resolution.
	Cycles *CycleGraph
}

type CondensedGraph struct {
	Nodes map[SCCID]SCCNode
	Edges map[SCCID][]SCCEdge
}

type DependencyGraphResult struct {
	ObjectGraph    ObjectGraph
	CondensedGraph CondensedGraph
	ObjectToSCC    map[ObjectID]SCCID
}

// HasCyclesFor reports whether any cycle exists among the given object IDs.
// It traverses the pre-computed CondensedGraph: any SCC node with Cycles != nil
// that contains at least one in-scope member indicates a cycle.
func (r DependencyGraphResult) HasCyclesFor(ids map[ObjectID]struct{}) bool {
	for _, node := range r.CondensedGraph.Nodes {
		if node.Cycles == nil {
			continue
		}
		for _, memberID := range node.Members {
			if _, ok := ids[memberID]; ok {
				return true
			}
		}
	}
	return false
}

type CycleID string
type CycleGroupID string
type CycleIndex int

type Cycle struct {
	ID    CycleID
	Edges []ObjectEdge
}

type CycleGroup struct {
	ID CycleGroupID

	Cycles []CycleIndex

	Members []ObjectID
}

type CycleGroupEdge struct {
	From CycleGroupID
	To   CycleGroupID

	SharedObjects []ObjectID

	Links []ObjectEdge
}

type CycleGraph struct {
	Cycles []Cycle

	Groups map[CycleGroupID]CycleGroup

	GroupGraph map[CycleGroupID][]CycleGroupEdge
}
