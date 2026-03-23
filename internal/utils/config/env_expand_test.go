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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpandEnvVars(t *testing.T) {
	t.Run("set variable is expanded", func(t *testing.T) {
		t.Setenv("GM_TEST_LEVEL", "debug")
		result, err := ExpandEnvVars("level: ${GM_TEST_LEVEL}")
		require.NoError(t, err)
		assert.Equal(t, "level: debug", result)
	})

	t.Run("unset variable with default uses default", func(t *testing.T) {
		result, err := ExpandEnvVars("level: ${GM_UNSET_VAR:-info}")
		require.NoError(t, err)
		assert.Equal(t, "level: info", result)
	})

	t.Run("unset variable with explicit empty default returns empty string", func(t *testing.T) {
		result, err := ExpandEnvVars("prefix: ${GM_UNSET_VAR:-}")
		require.NoError(t, err)
		assert.Equal(t, "prefix: ", result)
	})

	t.Run("unset variable without default returns error", func(t *testing.T) {
		_, err := ExpandEnvVars("level: ${GM_UNSET_REQUIRED}")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "GM_UNSET_REQUIRED")
		assert.Contains(t, err.Error(), "not set")
	})

	t.Run("empty variable treated as unset, uses default", func(t *testing.T) {
		t.Setenv("GM_TEST_EMPTY", "")
		result, err := ExpandEnvVars("level: ${GM_TEST_EMPTY:-warn}")
		require.NoError(t, err)
		assert.Equal(t, "level: warn", result)
	})

	t.Run("empty variable without default returns error", func(t *testing.T) {
		t.Setenv("GM_TEST_EMPTY2", "")
		_, err := ExpandEnvVars("level: ${GM_TEST_EMPTY2}")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "GM_TEST_EMPTY2")
	})

	t.Run("multiple variables in one string", func(t *testing.T) {
		t.Setenv("GM_TEST_USER", "admin")
		t.Setenv("GM_TEST_HOST", "localhost")
		result, err := ExpandEnvVars("dbname: host=${GM_TEST_HOST} user=${GM_TEST_USER}")
		require.NoError(t, err)
		assert.Equal(t, "dbname: host=localhost user=admin", result)
	})

	t.Run("multiple variables, one unset without default returns error", func(t *testing.T) {
		t.Setenv("GM_TEST_SET", "value")
		_, err := ExpandEnvVars("${GM_TEST_SET} and ${GM_UNSET_NO_DEFAULT}")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "GM_UNSET_NO_DEFAULT")
	})

	t.Run("escape $${VAR} produces literal ${VAR}", func(t *testing.T) {
		result, err := ExpandEnvVars("template: $${MY_VAR}")
		require.NoError(t, err)
		assert.Equal(t, "template: ${MY_VAR}", result)
	})

	t.Run("go template syntax is not affected", func(t *testing.T) {
		input := `min: '{{ now | tsModify "-30 years" | .EncodeValue }}'`
		result, err := ExpandEnvVars(input)
		require.NoError(t, err)
		assert.Equal(t, input, result)
	})

	t.Run("content without any placeholders is unchanged", func(t *testing.T) {
		input := "level: info\nformat: text\npath: /tmp/greenmask"
		result, err := ExpandEnvVars(input)
		require.NoError(t, err)
		assert.Equal(t, input, result)
	})

	t.Run("set variable overrides default", func(t *testing.T) {
		t.Setenv("GM_TEST_FORMAT", "json")
		result, err := ExpandEnvVars("format: ${GM_TEST_FORMAT:-text}")
		require.NoError(t, err)
		assert.Equal(t, "format: json", result)
	})

	t.Run("variable embedded in larger string", func(t *testing.T) {
		t.Setenv("GM_TEST_BUCKET", "my-bucket")
		result, err := ExpandEnvVars("endpoint: http://s3.example.com/${GM_TEST_BUCKET}/prefix")
		require.NoError(t, err)
		assert.Equal(t, "endpoint: http://s3.example.com/my-bucket/prefix", result)
	})

	t.Run("multiline yaml content", func(t *testing.T) {
		t.Setenv("GM_TEST_LOG_LEVEL", "warn")
		t.Setenv("GM_TEST_DB", "mydb")
		input := "log:\n  level: ${GM_TEST_LOG_LEVEL}\ndump:\n  pg_dump_options:\n    dbname: ${GM_TEST_DB:-demo}"
		result, err := ExpandEnvVars(input)
		require.NoError(t, err)
		assert.Equal(t, "log:\n  level: warn\ndump:\n  pg_dump_options:\n    dbname: mydb", result)
	})
}
