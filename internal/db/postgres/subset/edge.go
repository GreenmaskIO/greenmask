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
