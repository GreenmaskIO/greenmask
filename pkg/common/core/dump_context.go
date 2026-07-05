package core

type ExplicitDumpContextInput struct {
	Config              any
	TableConfigs        []TableConfig
	IntrospectionResult IntrospectionResult
	// AllowedObjects is the scoped set produced by ObjectFilter.
	// The builder iterates only over objects whose ID appears here.
	// A nil or empty map means all objects are allowed.
	AllowedObjects      map[ObjectKind][]ObjectID
	Subset              SubsetResult
	SchemaDrift         SchemaDriftResult
	TransformerRegistry TransformerRegistry
}

type DerivedDumpContextInput struct {
	Config              any
	TableConfigs        []TableConfig
	IntrospectionResult IntrospectionResult
	// AllowedObjects is the scoped set produced by ObjectFilter.
	// The builder iterates only over objects whose ID appears here.
	// A nil or empty map means all objects are allowed.
	AllowedObjects        map[ObjectKind][]ObjectID
	Subset                SubsetResult
	DependencyGraphResult DependencyGraphResult
	SchemaDrift           SchemaDriftResult
	ExplicitCtx           DumpContext
}
