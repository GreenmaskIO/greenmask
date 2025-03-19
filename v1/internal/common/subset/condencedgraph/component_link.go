package condencedgraph

type ComponentLink struct {
	idx       int
	component SCC
}

func NewComponentLink(idx int, c SCC) ComponentLink {
	return ComponentLink{
		idx:       idx,
		component: c,
	}
}
