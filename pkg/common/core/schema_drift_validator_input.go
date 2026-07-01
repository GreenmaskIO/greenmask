package core

type SchemaDriftValidatorInput struct {
	Previous IntrospectionResult
	Current  IntrospectionResult
}
