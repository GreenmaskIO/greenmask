package subset

type ScopeEdge struct {
	scopeId               int
	originalCondensedEdge *CondensedEdge
	isNullable            bool
}
