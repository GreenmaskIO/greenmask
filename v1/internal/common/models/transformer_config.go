package models

import "maps"

type TransformerConfig struct {
	Name               string                       `json:"name"`
	ApplyForReferences bool                         `json:"apply_for_references"`
	StaticParams       map[string]ParamsValue       `json:"static_params"`
	DynamicParams      map[string]DynamicParamValue `json:"dynamic_params"`
	When               string                       `json:"when,omitempty"`
}

func NewTransformerConfig(
	name string,
	applyForReferences bool,
	params map[string]ParamsValue,
	dynamicParams map[string]DynamicParamValue,
	when string,
) TransformerConfig {
	return TransformerConfig{
		Name:               name,
		ApplyForReferences: applyForReferences,
		StaticParams:       params,
		DynamicParams:      dynamicParams,
		When:               when,
	}
}

func (tc *TransformerConfig) Clone() *TransformerConfig {
	return &TransformerConfig{
		Name:               tc.Name,
		ApplyForReferences: tc.ApplyForReferences,
		StaticParams:       maps.Clone(tc.StaticParams),
		DynamicParams:      maps.Clone(tc.DynamicParams),
		When:               tc.When,
	}
}
