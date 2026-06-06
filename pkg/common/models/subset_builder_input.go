package models

type SubsetBuilderInput struct {
	Introspection   IntrospectionResult
	DependencyGraph DependencyGraphResult
	TableConfigs    []TableConfig
}

type SubsetResult struct {
	SubsetMap map[ObjectID]string
}
