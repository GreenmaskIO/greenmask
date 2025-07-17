package models

type TableConfig struct {
	Schema              string              `json:"schema"`
	Name                string              `json:"name"`
	Query               string              `json:"query"`
	ApplyForInherited   bool                `json:"apply_for_inherited"`
	Transformers        []TransformerConfig `json:"transformers"`
	ColumnsTypeOverride map[string]string   `json:"columns_type_override"`
	SubsetConds         []string            `json:"subset_conds"`
	When                string              `json:"when"`
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
