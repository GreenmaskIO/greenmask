package core

type DumpPlanValidationInput struct {
	Plan DumpPlan
	// DependencyGraph is the object dependency graph built over the full,
	// pre-filter introspection. It is the authoritative source of foreign-key
	// edges and lets the plan validator cross-check the selected dump set against
	// the schema's referential structure (e.g. a dumped child whose referenced
	// parent was filtered out). Carried on the validation input rather than on the
	// durable DumpPlan so the plan artifact is not bloated with the graph.
	DependencyGraph DependencyGraphResult
}
