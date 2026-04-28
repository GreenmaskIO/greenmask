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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
)

func TestConnectionOpts_Params(t *testing.T) {
	t.Run("basic tcp params", func(t *testing.T) {
		opts := ConnectionOpts{
			User: "root",
			Host: "db.example.com",
			Port: 3306,
		}
		params := opts.Params(commonconfig.SSLOpts{})
		assert.Contains(t, params, "--user")
		assert.Contains(t, params, "root")
		assert.Contains(t, params, "--host")
		assert.Contains(t, params, "db.example.com")
		assert.Contains(t, params, "--port")
		assert.Contains(t, params, "3306")
	})

	t.Run("localhost is rewritten to 127.0.0.1", func(t *testing.T) {
		opts := ConnectionOpts{Host: "localhost", Port: 3306}
		params := opts.Params(commonconfig.SSLOpts{})
		assert.Contains(t, params, "127.0.0.1")
		assert.NotContains(t, params, "localhost")
	})

	t.Run("socket replaces host/port", func(t *testing.T) {
		opts := ConnectionOpts{
			Host:   "localhost",
			Port:   3306,
			Socket: "/var/run/mysqld/mysqld.sock",
		}
		params := opts.Params(commonconfig.SSLOpts{})
		assert.Contains(t, params, "--socket")
		assert.Contains(t, params, "/var/run/mysqld/mysqld.sock")
		assert.NotContains(t, params, "--host")
		assert.NotContains(t, params, "--port")
	})

	t.Run("ssl_mode emitted", func(t *testing.T) {
		opts := ConnectionOpts{}
		params := opts.Params(commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyCA})
		assert.Contains(t, params, "--ssl-mode")
		assert.Contains(t, params, "VERIFY_CA")
	})

	t.Run("ssl_mode disabled emitted explicitly", func(t *testing.T) {
		opts := ConnectionOpts{}
		params := opts.Params(commonconfig.SSLOpts{Mode: commonconfig.SSLModeDisabled})
		assert.Contains(t, params, "--ssl-mode")
		assert.Contains(t, params, "DISABLED")
	})

	t.Run("all ssl flags", func(t *testing.T) {
		opts := ConnectionOpts{}
		ssl := commonconfig.SSLOpts{
			Mode:            commonconfig.SSLModeVerifyIdentity,
			CA:              "/path/ca.pem",
			CAPath:          "/path/cadir",
			Cert:            "/path/cert.pem",
			Key:             "/path/key.pem",
			Cipher:          "AES128-SHA",
			TLSVersion:      "TLSv1.3",
			TLSCipherSuites: "TLS_AES_128_GCM_SHA256",
		}
		params := opts.Params(ssl)
		assert.Contains(t, params, "--ssl-ca")
		assert.Contains(t, params, "/path/ca.pem")
		assert.Contains(t, params, "--ssl-capath")
		assert.Contains(t, params, "/path/cadir")
		assert.Contains(t, params, "--ssl-cert")
		assert.Contains(t, params, "/path/cert.pem")
		assert.Contains(t, params, "--ssl-key")
		assert.Contains(t, params, "/path/key.pem")
		assert.Contains(t, params, "--ssl-cipher")
		assert.Contains(t, params, "AES128-SHA")
		assert.Contains(t, params, "--tls-version")
		assert.Contains(t, params, "TLSv1.3")
		assert.Contains(t, params, "--tls-ciphersuites")
		assert.Contains(t, params, "TLS_AES_128_GCM_SHA256")
	})

	t.Run("no ssl flags when mode empty", func(t *testing.T) {
		opts := ConnectionOpts{Host: "localhost", Port: 3306}
		params := opts.Params(commonconfig.SSLOpts{})
		assert.NotContains(t, params, "--ssl-mode")
		assert.NotContains(t, params, "--ssl-ca")
	})
}

func TestConnectionOpts_ConnectionConfig(t *testing.T) {
	t.Run("returns ConnConfig with correct fields", func(t *testing.T) {
		opts := ConnectionOpts{
			User:            "user1",
			Password:        "pass1",
			Host:            "db.host",
			Port:            3307,
			ConnectDatabase: "mydb",
		}
		cc, err := opts.ConnectionConfig(commonconfig.SSLOpts{})
		require.NoError(t, err)
		require.NotNil(t, cc)
	})

	t.Run("tls config error propagates", func(t *testing.T) {
		opts := ConnectionOpts{
			Host: "localhost",
			Port: 3306,
		}
		ssl := commonconfig.SSLOpts{
			Mode: commonconfig.SSLModeVerifyCA,
			CA:   "/nonexistent/ca.pem",
		}
		_, err := opts.ConnectionConfig(ssl)
		require.Error(t, err)
	})
}
