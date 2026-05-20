package models

type ExplicitDumpContextInput struct {
	Config              any
	TableConfigs        []TableConfig
	IntrospectionResult IntrospectionResult
	SubsetMapping       map[ObjectID]string
}

func (m ExplicitDumpContextInput) ToDerivedDumpContextInput(
	graph DependencyGraphResult,
) DerivedDumpContextInput {
	return DerivedDumpContextInput{
		Config:                m.Config,
		TableConfigs:          m.TableConfigs,
		IntrospectionResult:   m.IntrospectionResult,
		SubsetMapping:         m.SubsetMapping,
		DependencyGraphResult: graph,
	}
}

type DerivedDumpContextInput struct {
	Config                any
	TableConfigs          []TableConfig
	IntrospectionResult   IntrospectionResult
	SubsetMapping         map[ObjectID]string
	DependencyGraphResult DependencyGraphResult
}
