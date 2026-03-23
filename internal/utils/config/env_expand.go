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
	"regexp"
	"strings"
)

// envVarPattern matches two forms:
//   - $${...}   — escape sequence, produces literal ${...}
//   - ${VAR}    — expand to the value of the VAR environment variable
//   - ${VAR:-default} — expand to VAR, or "default" if VAR is unset/empty
var envVarPattern = regexp.MustCompile(`\$\$\{[^}]*\}|\$\{([^}:]+)(?::-([^}]*))?}`)

// ExpandEnvVars replaces ${VAR} and ${VAR:-default} placeholders in content
// with the corresponding environment variable values.
//
// Rules:
//   - ${VAR}          — replaced with the value of VAR; returns an error if VAR is unset or empty
//   - ${VAR:-default} — replaced with the value of VAR, or "default" if VAR is unset or empty
//   - ${VAR:-}        — replaced with an empty string when VAR is unset or empty (explicit empty default)
//   - $${VAR}         — escape sequence; produces the literal string "${VAR}" without env lookup
//
// Greenmask transformer templates use {{ }} syntax and are not affected by this function.
func ExpandEnvVars(content string) (string, error) {
	var expandErr error
	result := envVarPattern.ReplaceAllStringFunc(content, func(match string) string {
		if expandErr != nil {
			return match
		}

		// $${...} is an escape sequence — strip one $ to produce the literal ${...}
		if strings.HasPrefix(match, "$$") {
			return match[1:]
		}

		sub := envVarPattern.FindStringSubmatch(match)
		varName := sub[1]
		hasDefault := strings.Contains(match, ":-")
		defaultVal := sub[2]

		if value, ok := os.LookupEnv(varName); ok && value != "" {
			return value
		}

		if hasDefault {
			return defaultVal
		}

		expandErr = fmt.Errorf(
			"environment variable %q is not set; use ${%s:-fallback} to provide a default value",
			varName, varName,
		)
		return match
	})
	return result, expandErr
}
