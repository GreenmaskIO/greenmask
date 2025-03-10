package models

type TableConfig struct {
	Schema              string
	Name                string
	Query               string
	ApplyForInherited   bool
	Transformers        Transformers
	ColumnsTypeOverride map[string]string
	SubsetConds         []string
	When                string
}

func NewTableConfig(
	schema string,
	name string,
	query string,
	applyForInherited bool,
	transformers []TransformerConfig,
	columnsTypeOverride map[string]string,
	subsetConds []string,
	when string,
) TableConfig {
	return TableConfig{
		Schema:              schema,
		Name:                name,
		Query:               query,
		ApplyForInherited:   applyForInherited,
		Transformers:        transformers,
		ColumnsTypeOverride: columnsTypeOverride,
		SubsetConds:         subsetConds,
		When:                when,
	}
}
