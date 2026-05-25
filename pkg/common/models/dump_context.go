package models

type ExplicitDumpContextInput struct {
	Config              any
	TableConfigs        []TableConfig
	IntrospectionResult IntrospectionResult
	Subset              SubsetResult
	SchemaDrift         SchemaDriftResult
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
