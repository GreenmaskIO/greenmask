package core

type ConfigEditInput struct {
	Config              []TableConfig
	IntrospectionResult IntrospectionResult
	SchemaDrift         *SchemaDriftResult
}
