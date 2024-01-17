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

import (
	"encoding/json"
	"fmt"
)

type Oid int
type AttNum uint32

type DynamicParameters map[string]*DynamicParamValue

type ParameterValuer interface {
	Value()
}

type DynamicParamValue struct {
	Column       string      `mapstructure:"column" json:"column,omitempty"`
	CastTemplate string      `mapstructure:"cast_template" json:"cast_template,omitempty"`
	DefaultValue ParamsValue `mapstructure:"default_value" json:"default_value,omitempty"`
}

func (dpv *DynamicParamValue) ParameterValuer() {}

type ParamsValue []byte

func (pv *ParamsValue) ParameterValuer() {}

func (pv *ParamsValue) UnmarshalJSON(data []byte) error {
	var val any
	err := json.Unmarshal(data, &val)
	if err != nil {
		return fmt.Errorf("error unmarshallinbg ParamsValue: %w", err)
	}
	switch v := val.(type) {
	case string:
		*pv = []byte(v)
	default:
		*pv = data
	}
	return nil
}

type StaticParameters map[string]ParamsValue

func (p *StaticParameters) MarshalJSON() ([]byte, error) {
	castedMap := make(map[string]any)

	for k, v := range *p {
		var val any
		err := json.Unmarshal(v, &val)
		if err == nil {
			castedMap[k] = val
		} else {
			castedMap[k] = string(v)
		}
	}

	res, err := json.Marshal(castedMap)
	if err != nil {
		return nil, err
	}
	return res, nil
}
