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
