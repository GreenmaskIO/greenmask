package subset

import "container/list"

type QueryBuilder struct {
	List *list.List
}

func NewQueryBuilder([]*Table) *QueryBuilder {
	return &QueryBuilder{
		List: list.New(),
	}
}
