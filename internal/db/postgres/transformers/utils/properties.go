// Copyright 2023 Greenmask
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

package utils

type MetaKey string

type TransformerProperties struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	IsCustom    bool            `json:"is_custom"`
	Meta        map[MetaKey]any `json:"meta"`
}

func NewTransformerProperties(
	name, description string,
) *TransformerProperties {
	return &TransformerProperties{
		Name:        name,
		Description: description,
		Meta:        make(map[MetaKey]any),
	}
}

func (tp *TransformerProperties) AddMeta(key MetaKey, value any) *TransformerProperties {
	tp.Meta[key] = value
	return tp
}

func (tp *TransformerProperties) GetMeta(key MetaKey) (any, bool) {
	value, ok := tp.Meta[key]
	return value, ok
}
