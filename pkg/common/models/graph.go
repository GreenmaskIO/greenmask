package models

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
