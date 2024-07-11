package subset

type Edge struct {
	Id  int
	Idx int
	A   *TableLink
	B   *TableLink
}

func NewEdge(id, idx int, a *TableLink, b *TableLink) *Edge {
	return &Edge{
		Id:  id,
		Idx: idx,
		A:   a,
		B:   b,
	}
}

func (e *Edge) GetLeftAndRightTable(idx int) (*TableLink, *TableLink) {
	if e.A.Idx == idx {
		return e.A, e.B
	}
	return e.B, e.A
}
