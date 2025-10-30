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

package config

import (
	"encoding/json"
	"fmt"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type ParamsValue []byte

func (pv *ParamsValue) ToParamsValue() models.ParamsValue {
	return models.NewParamsValue(*pv)
}

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

func (p *StaticParameters) ToParamsValue() map[string]models.ParamsValue {
	res := make(map[string]models.ParamsValue, len(*p))
	for k, v := range *p {
		res[k] = v.ToParamsValue()
	}
	return res
}

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

type DynamicParameters map[string]DynamicParamValue

func (dp *DynamicParameters) ToDynamicParamValue() map[string]models.DynamicParamValue {
	res := make(map[string]models.DynamicParamValue, len(*dp))
	for k, v := range *dp {
		res[k] = models.NewDynamicParamValue(
			v.Column,
			v.CastTo,
			v.Template,
			v.DefaultValue.ToParamsValue(),
		)
	}
	return res
}

type DynamicParamValue struct {
	Column       string      `mapstructure:"column" json:"column,omitempty"`
	CastTo       string      `mapstructure:"cast_to" json:"cast_to,omitempty"`
	Template     string      `mapstructure:"template" json:"template,omitempty"`
	DefaultValue ParamsValue `mapstructure:"default_value" json:"default_value,omitempty"`
}

func (dpv *DynamicParamValue) ToDynamicParamValue() models.DynamicParamValue {
	return models.NewDynamicParamValue(
		dpv.Column,
		dpv.CastTo,
		dpv.Template,
		dpv.DefaultValue.ToParamsValue(),
	)
}
