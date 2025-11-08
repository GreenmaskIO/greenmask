// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
