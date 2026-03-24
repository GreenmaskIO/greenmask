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
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/buildkite/interpolate"
	"github.com/go-viper/mapstructure/v2"
)

// InterpolateEnvVars expands environment variable references in s using POSIX
// Parameter Expansion syntax. Returns s unchanged if it contains no '$'.
//
// Supported syntax:
//   - ${VAR}          — value of VAR; empty string if VAR is unset
//   - $VAR            — same as ${VAR}
//   - ${VAR:-default} — value of VAR, or "default" if VAR is unset or empty
//   - ${VAR-default}  — value of VAR, or "default" if VAR is unset (but not if empty)
//   - ${VAR?message}  — value of VAR; error with "message" if VAR is unset
//   - $$VAR           — escape sequence, produces the literal string $VAR
func InterpolateEnvVars(s string) (string, error) {
	if !strings.Contains(s, "$") {
		return s, nil
	}
	env := interpolate.NewSliceEnv(os.Environ())
	res, err := interpolate.Interpolate(env, s)
	if err != nil {
		return "", fmt.Errorf("interpolate environment variable: %w", err)
	}
	return res, nil
}

// InterpolateEnvVarsHookFunc returns a mapstructure DecodeHookFunc that expands
// environment variable references in every string field during config unmarshaling.
func InterpolateEnvVarsHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		return InterpolateEnvVars(data.(string))
	}
}
