package models

import "maps"

type TransformerConfig struct {
	Name               string
	ApplyForReferences bool
	Params             map[string]ParamsValue
	DynamicParams      map[string]DynamicParamValue
	When               string
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
		Params:             params,
		DynamicParams:      dynamicParams,
		When:               when,
	}
}

func (tc *TransformerConfig) Clone() *TransformerConfig {
	return &TransformerConfig{
		Name:               tc.Name,
		ApplyForReferences: tc.ApplyForReferences,
		Params:             maps.Clone(tc.Params),
		DynamicParams:      maps.Clone(tc.DynamicParams),
		When:               tc.When,
	}
}
