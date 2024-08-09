package subset

type CondensedEdge struct {
	id           int
	from         *ComponentLink
	to           *ComponentLink
	originalEdge *Edge
}

func NewCondensedEdge(id int, from, to *ComponentLink, originalEdge *Edge) *CondensedEdge {
	return &CondensedEdge{
		id:           id,
		from:         from,
		to:           to,
		originalEdge: originalEdge,
	}
}
