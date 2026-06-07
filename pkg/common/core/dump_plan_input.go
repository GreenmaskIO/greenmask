package core

type DumpPlanInput struct {
	DumpContext         DumpContext
	DumpContextSnapshot DumpContextSnapshot
	DumpContextDiff     DumpContextDiff
	RestorationContext  RestorationContext
	IntrospectionResult IntrospectionResult
	Config              []TableConfig
}
