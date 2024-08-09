package subset

type ComponentLink struct {
	idx       int
	component *Component
}

func NewComponentLink(idx int, c *Component, keys, overriddenKeys []string) *ComponentLink {
	return &ComponentLink{
		idx:       idx,
		component: c,
	}
}
