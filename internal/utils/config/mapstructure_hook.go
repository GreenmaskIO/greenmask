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

func StringToSliceWithBracketHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Kind,
		t reflect.Kind,
		data interface{}) (interface{}, error) {
		if f != reflect.String || t != reflect.Slice {
			return data, nil
		}

		raw := data.(string)
		if raw == "" {
			return []string{}, nil
		}
		var slice []json.RawMessage
		err := json.Unmarshal([]byte(raw), &slice)
		if err != nil {
			return data, nil
		}

		var strSlice []string
		for _, v := range slice {
			strSlice = append(strSlice, string(v))
		}
		return strSlice, nil
	}
}

func StringToStructHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f.Kind() != reflect.String ||
			(t.Kind() != reflect.Struct && !(t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct)) {
			return data, nil
		}
		raw := data.(string)
		var val reflect.Value
		// Struct or the pointer to a struct
		if t.Kind() == reflect.Struct {
			val = reflect.New(t)
		} else {
			val = reflect.New(t.Elem())
		}

		if raw == "" {
			return val, nil
		}
		err := json.Unmarshal([]byte(raw), val.Interface())
		if err != nil {
			return data, nil
		}
		return val.Interface(), nil
	}
}
