package models

type ConfigEditInput struct {
	Config              []TableConfig
	IntrospectionResult IntrospectionResult
	SchemaDrift         *SchemaDriftResult
}
