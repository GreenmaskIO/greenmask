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

package toolkit

type Meta struct {
	Table               *Table            `json:"table"`
	Parameters          *Parameters       `json:"parameters"`
	Types               []*Type           `json:"types"`
	ColumnsTypeOverride map[string]string `json:"columns_type_override"`
}

type Parameters struct {
	Static  StaticParameters  `json:"static,omitempty"`
	Dynamic DynamicParameters `json:"dynamic,omitempty"`
}
