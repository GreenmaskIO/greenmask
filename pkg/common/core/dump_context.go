package core

type ExplicitDumpContextInput struct {
	Config              any
	TableConfigs        []TableConfig
	IntrospectionResult IntrospectionResult
	Subset              SubsetResult
	SchemaDrift         SchemaDriftResult
	TransformerRegistry TransformerRegistry
}

type DerivedDumpContextInput struct {
	Config                any
	TableConfigs          []TableConfig
	IntrospectionResult   IntrospectionResult
	Subset                SubsetResult
	DependencyGraphResult DependencyGraphResult
	SchemaDrift           SchemaDriftResult
	ExplicitCtx           DumpContext
}
