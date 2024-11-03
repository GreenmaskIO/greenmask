package subset

type Edge struct {
	id         int
	idx        int
	isNullable bool
	from       *TableLink
	to         *TableLink
}

func NewEdge(id, idx int, isNullable bool, a *TableLink, b *TableLink) *Edge {
	return &Edge{
		id:         id,
		idx:        idx,
		isNullable: isNullable,
		from:       a,
		to:         b,
	}
}

func (e *Edge) ID() int {
	return e.id
}

func (e *Edge) Index() int {
	return e.idx
}

func (e *Edge) IsNullable() bool {
	return e.isNullable
}

func (e *Edge) From() *TableLink {
	return e.from
}

func (e *Edge) To() *TableLink {
	return e.to
}
