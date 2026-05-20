package models

type DumpProcessorInput struct {
	DumpContext         DumpContext
	IntrospectionResult IntrospectionResult
	Config              any
}
