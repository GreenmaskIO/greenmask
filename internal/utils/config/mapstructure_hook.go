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

package config

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/mitchellh/mapstructure"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func ParamsToByteSliceHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if t != reflect.TypeOf(toolkit.ParamsValue{}) {
			return data, nil
		}

		switch v := data.(type) {
		case string:
			return toolkit.ParamsValue(v), nil
		default:
			res, err := json.Marshal(data)
			if err != nil {
				return nil, fmt.Errorf("cannot convert object to json bytes: %w", err)
			}
			return res, nil
		}
	}
}
