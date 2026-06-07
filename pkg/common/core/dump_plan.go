package core

type DumpPlan struct {
	DumpObjectSpecs    []ObjectDumpSpec
	SchemaDumpSpecs    []SchemaDumpSpec
	RestorationContext RestorationContext
	// TransformationConfig - transformation config, included inherited transformers
	// and those that has been automatically applied.
	TransformationConfig []TableConfig
	MatchedDatabases     []string
	Tags                 []string
	Description          string
	IntrospectionResult  IntrospectionResult
}
