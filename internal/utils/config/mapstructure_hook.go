package config

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/mitchellh/mapstructure"
)

func ParamsToByteSliceHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if t != reflect.TypeOf([]byte{}) {
			return data, nil
		}

		switch v := data.(type) {
		case string:
			return []byte(v), nil
		default:
			res, err := json.Marshal(data)
			if err != nil {
				return nil, fmt.Errorf("cannot convert map to yaml bytes: %w", err)
			}
			return res, nil
		}
	}
}
