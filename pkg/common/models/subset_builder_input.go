package models

type SubsetBuilderInput struct {
	Introspection   IntrospectionResult
	DependencyGraph DependencyGraphResult
}

type SubsetResult struct {
	SubsetMap map[ObjectID]string
}
