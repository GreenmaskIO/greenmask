package subset

type cyclesQueryBuilder struct {
	sg      *subsetGraph
	dialect Dialect
}

func newCyclesQueryBuilder(sg *subsetGraph, dialect Dialect) cyclesQueryBuilder {
	return cyclesQueryBuilder{
		sg:      sg,
		dialect: dialect,
	}
}

func (b cyclesQueryBuilder) build() (map[int]string, error) {
	panic("implement me")
}
