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

package models

import (
	"crypto/tls"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
)

func TestConnConfig_URI(t *testing.T) {
	t.Run("plain tcp DSN", func(t *testing.T) {
		cfg := &ConnConfig{
			User: "root", Password: "pass",
			Host: "db.host", Port: 3306,
			Database: "mydb",
		}
		uri, err := cfg.URI()
		require.NoError(t, err)
		assert.Equal(t, "root:pass@tcp(db.host:3306)/mydb", uri)
	})

	t.Run("socket DSN uses unix network", func(t *testing.T) {
		cfg := &ConnConfig{
			User: "root", Password: "pass",
			Socket:   "/var/run/mysqld/mysqld.sock",
			Database: "mydb",
		}
		uri, err := cfg.URI()
		require.NoError(t, err)
		assert.Equal(t, "root:pass@unix(/var/run/mysqld/mysqld.sock)/mydb", uri)
	})

	t.Run("maxAllowedPacket added as query param", func(t *testing.T) {
		cfg := &ConnConfig{
			User: "root", Password: "pass",
			Host: "host", Port: 3306,
			Database:         "db",
			MaxAllowedPacket: 1073741824,
		}
		uri, err := cfg.URI()
		require.NoError(t, err)
		assert.Contains(t, uri, "maxAllowedPacket=1073741824")
	})

	t.Run("ssl_mode=DISABLED adds tls=false", func(t *testing.T) {
		cfg := &ConnConfig{
			User: "root", Password: "pass",
			Host: "host", Port: 3306,
			Database: "db",
			SSLMode:  commonconfig.SSLModeDisabled,
		}
		uri, err := cfg.URI()
		require.NoError(t, err)
		assert.Contains(t, uri, "tls=false")
	})

	t.Run("ssl_mode=PREFERRED adds tls=preferred", func(t *testing.T) {
		cfg := &ConnConfig{
			User: "root", Password: "pass",
			Host: "host", Port: 3306,
			Database: "db",
			SSLMode:  commonconfig.SSLModePreferred,
		}
		uri, err := cfg.URI()
		require.NoError(t, err)
		assert.Contains(t, uri, "tls=preferred")
	})

	t.Run("ssl_mode=REQUIRED adds tls=skip-verify", func(t *testing.T) {
		cfg := &ConnConfig{
			User: "root", Password: "pass",
			Host: "host", Port: 3306,
			Database: "db",
			SSLMode:  commonconfig.SSLModeRequired,
		}
		uri, err := cfg.URI()
		require.NoError(t, err)
		assert.Contains(t, uri, "tls=skip-verify")
	})

	t.Run("ssl_mode=VERIFY_CA with TLSConfig registers named config", func(t *testing.T) {
		cfg := &ConnConfig{
			User: "root", Password: "pass",
			Host: "host", Port: 3306,
			Database:  "db",
			SSLMode:   commonconfig.SSLModeVerifyCA,
			TLSConfig: &tls.Config{InsecureSkipVerify: false}, //nolint:gosec
		}
		uri, err := cfg.URI()
		require.NoError(t, err)
		// Should contain tls=<registered-key>, not tls=true or tls=false.
		assert.True(t, strings.Contains(uri, "tls=greenmask-"), "DSN should contain named tls config key, got: %s", uri)
	})

	t.Run("no ssl mode produces no tls param", func(t *testing.T) {
		cfg := &ConnConfig{
			User: "root", Password: "pass",
			Host: "host", Port: 3306,
			Database: "db",
		}
		uri, err := cfg.URI()
		require.NoError(t, err)
		assert.NotContains(t, uri, "tls=")
	})

	t.Run("multiple params joined with &", func(t *testing.T) {
		cfg := &ConnConfig{
			User: "root", Password: "pass",
			Host: "host", Port: 3306,
			Database:         "db",
			SSLMode:          commonconfig.SSLModeDisabled,
			MaxAllowedPacket: 1024,
		}
		uri, err := cfg.URI()
		require.NoError(t, err)
		assert.Contains(t, uri, "maxAllowedPacket=1024")
		assert.Contains(t, uri, "tls=false")
		assert.Contains(t, uri, "&")
	})
}

func TestConnConfig_Address(t *testing.T) {
	t.Run("tcp address", func(t *testing.T) {
		cfg := &ConnConfig{Host: "myhost", Port: 3306}
		assert.Equal(t, "myhost:3306", cfg.Address())
	})

	t.Run("socket address", func(t *testing.T) {
		cfg := &ConnConfig{Socket: "/var/run/mysqld/mysqld.sock"}
		assert.Equal(t, "/var/run/mysqld/mysqld.sock", cfg.Address())
	})
}
