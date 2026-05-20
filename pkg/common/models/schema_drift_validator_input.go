package models

type SchemaDriftValidatorInput struct {
	Previous IntrospectionResult
	Current  IntrospectionResult
}
