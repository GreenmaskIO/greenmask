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
	"strings"
	"testing"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
)

// TestCommonDumpOptions_SSLSquash verifies that the squash tag on the SSL field
// correctly promotes SSLOpts fields to the CommonDumpOptions level for mapstructure
// decoding (the path used by viper.Unmarshal).
//
// The key risk is ErrorUnused: true — if mapstructure doesn't recognise the ssl_*
// keys as belonging to the squashed SSLOpts, it will return an "unused keys" error
// on any config that contains ssl_mode / ssl_ca / etc.
func TestCommonDumpOptions_SSLSquash(t *testing.T) {
	decode := func(t *testing.T, input map[string]any) (CommonDumpOptions, error) {
		t.Helper()
		var out CommonDumpOptions
		dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
			Result:      &out,
			ErrorUnused: true,
			TagName:     "mapstructure",
		})
		require.NoError(t, err)
		return out, dec.Decode(input)
	}

	t.Run("ssl_mode decoded at options level", func(t *testing.T) {
		out, err := decode(t, map[string]any{
			"ssl_mode": "REQUIRED",
		})
		require.NoError(t, err)
		assert.Equal(t, commonconfig.SSLModeRequired, out.SSL.Mode)
	})

	t.Run("all ssl fields decoded without error_unused", func(t *testing.T) {
		out, err := decode(t, map[string]any{
			"ssl_mode":         "VERIFY_CA",
			"ssl_ca":           "/etc/ssl/ca.pem",
			"ssl_capath":       "/etc/ssl/certs",
			"ssl_cert":         "/etc/ssl/client-cert.pem",
			"ssl_key":          "/etc/ssl/client-key.pem",
			"ssl_cipher":       "AES128-SHA",
			"tls_version":      "TLSv1.2",
			"tls_ciphersuites": "TLS_AES_128_GCM_SHA256",
		})
		require.NoError(t, err, "ssl_* fields at options level must not trigger ErrorUnused")
		assert.Equal(t, commonconfig.SSLModeVerifyCA, out.SSL.Mode)
		assert.Equal(t, "/etc/ssl/ca.pem", out.SSL.CA)
		assert.Equal(t, "/etc/ssl/certs", out.SSL.CAPath)
		assert.Equal(t, "/etc/ssl/client-cert.pem", out.SSL.Cert)
		assert.Equal(t, "/etc/ssl/client-key.pem", out.SSL.Key)
		assert.Equal(t, "AES128-SHA", out.SSL.Cipher)
		assert.Equal(t, "TLSv1.2", out.SSL.TLSVersion)
		assert.Equal(t, "TLS_AES_128_GCM_SHA256", out.SSL.TLSCipherSuites)
	})

	t.Run("ssl and non-ssl fields coexist", func(t *testing.T) {
		out, err := decode(t, map[string]any{
			"ssl_mode":       "REQUIRED",
			"ssl_ca":         "/ca.pem",
			"schema-only":    true,
			"compress":       true,
			"include-schema": []any{"mydb"},
		})
		require.NoError(t, err)
		assert.Equal(t, commonconfig.SSLModeRequired, out.SSL.Mode)
		assert.Equal(t, "/ca.pem", out.SSL.CA)
		assert.True(t, out.SchemaOnly)
		assert.True(t, out.Compress)
		assert.Equal(t, []string{"mydb"}, out.IncludeSchema)
	})

	t.Run("empty ssl fields leave SSL zero-valued", func(t *testing.T) {
		out, err := decode(t, map[string]any{
			"compress": true,
		})
		require.NoError(t, err)
		assert.Equal(t, commonconfig.SSLOpts{}, out.SSL)
	})
}

// TestCommonDumpOptions_SSLSquash_ViaViper verifies the full production decode path:
// viper reads YAML → builds internal map → mapstructure.Unmarshal with ErrorUnused.
// This is how initConfig() works in cmd/mapstructure.go.
func TestCommonDumpOptions_SSLSquash_ViaViper(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		want    commonconfig.SSLOpts
		wantErr bool
	}{
		{
			name: "ssl_mode at options level",
			yaml: `
dump:
  options:
    ssl_mode: REQUIRED
`,
			want: commonconfig.SSLOpts{Mode: commonconfig.SSLModeRequired},
		},
		{
			name: "verify_ca with ca file",
			yaml: `
dump:
  options:
    ssl_mode: VERIFY_CA
    ssl_ca: /etc/ssl/ca.pem
`,
			want: commonconfig.SSLOpts{
				Mode: commonconfig.SSLModeVerifyCA,
				CA:   "/etc/ssl/ca.pem",
			},
		},
		{
			name: "all ssl fields",
			yaml: `
dump:
  options:
    ssl_mode: VERIFY_IDENTITY
    ssl_ca: /ca.pem
    ssl_capath: /cadir
    ssl_cert: /cert.pem
    ssl_key: /key.pem
    ssl_cipher: AES128-SHA
    tls_version: "TLSv1.2,TLSv1.3"
    tls_ciphersuites: TLS_AES_128_GCM_SHA256
`,
			want: commonconfig.SSLOpts{
				Mode:            commonconfig.SSLModeVerifyIdentity,
				CA:              "/ca.pem",
				CAPath:          "/cadir",
				Cert:            "/cert.pem",
				Key:             "/key.pem",
				Cipher:          "AES128-SHA",
				TLSVersion:      "TLSv1.2,TLSv1.3",
				TLSCipherSuites: "TLS_AES_128_GCM_SHA256",
			},
		},
		{
			name: "no ssl fields — zero value",
			yaml: `
dump:
  options:
    compress: true
`,
			want: commonconfig.SSLOpts{},
		},
	}

	decoderCfg := func(cfg *mapstructure.DecoderConfig) {
		cfg.ErrorUnused = true
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("yaml")
			require.NoError(t, v.ReadConfig(strings.NewReader(tc.yaml)))

			var cfg Config
			err := v.Unmarshal(&cfg, decoderCfg)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err, "ssl_* keys at options level must not trigger ErrorUnused")
			assert.Equal(t, tc.want, cfg.Dump.Options.SSL)
		})
	}
}
