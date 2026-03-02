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

type DynamicParamValue struct {
	Column       string      `json:"column"`
	CastTo       string      `json:"cast_to,omitempty"`
	Template     string      `json:"template,omitempty"`
	DefaultValue ParamsValue `json:"default_value,omitempty"`
}

func NewDynamicParamValue(
	column string,
	castTo string,
	template string,
	defaultValue ParamsValue,
) DynamicParamValue {
	return DynamicParamValue{
		Column:       column,
		CastTo:       castTo,
		Template:     template,
		DefaultValue: defaultValue,
	}
}
