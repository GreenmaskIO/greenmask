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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommonSSLOpts_TLSConfig(t *testing.T) {
	t.Run("disabled returns nil", func(t *testing.T) {
		s := SSLOpts{Mode: SSLModeDisabled}
		cfg, err := s.TLSConfig("host")
		require.NoError(t, err)
		assert.Nil(t, cfg)
	})

	t.Run("preferred returns nil", func(t *testing.T) {
		s := SSLOpts{Mode: SSLModePreferred}
		cfg, err := s.TLSConfig("host")
		require.NoError(t, err)
		assert.Nil(t, cfg)
	})

	t.Run("empty mode returns nil", func(t *testing.T) {
		s := SSLOpts{}
		cfg, err := s.TLSConfig("host")
		require.NoError(t, err)
		assert.Nil(t, cfg)
	})

	t.Run("required sets InsecureSkipVerify", func(t *testing.T) {
		s := SSLOpts{Mode: SSLModeRequired}
		cfg, err := s.TLSConfig("host")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.True(t, cfg.InsecureSkipVerify)
		assert.Nil(t, cfg.VerifyPeerCertificate)
	})

	t.Run("verify_ca sets InsecureSkipVerify and VerifyPeerCertificate", func(t *testing.T) {
		s := SSLOpts{Mode: SSLModeVerifyCA}
		cfg, err := s.TLSConfig("somehost")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.True(t, cfg.InsecureSkipVerify)
		assert.NotNil(t, cfg.VerifyPeerCertificate, "VerifyPeerCertificate must be set for CA chain validation")
		assert.Empty(t, cfg.ServerName)
	})

	t.Run("verify_identity sets ServerName", func(t *testing.T) {
		s := SSLOpts{Mode: SSLModeVerifyIdentity}
		cfg, err := s.TLSConfig("myserver.example.com")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.False(t, cfg.InsecureSkipVerify)
		assert.Equal(t, "myserver.example.com", cfg.ServerName)
	})

	t.Run("unknown mode returns error", func(t *testing.T) {
		s := SSLOpts{Mode: "BOGUS"}
		_, err := s.TLSConfig("host")
		require.Error(t, err)
	})

	t.Run("ssl_ca loaded into RootCAs", func(t *testing.T) {
		caFile := writeCommonTempCACert(t)
		s := SSLOpts{Mode: SSLModeVerifyCA, CA: caFile}
		cfg, err := s.TLSConfig("host")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.NotNil(t, cfg.RootCAs)
	})

	t.Run("missing ssl_ca file returns error", func(t *testing.T) {
		s := SSLOpts{Mode: SSLModeVerifyCA, CA: "/nonexistent/ca.pem"}
		_, err := s.TLSConfig("host")
		require.Error(t, err)
	})

	t.Run("ssl_ca with invalid PEM returns error", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "bad-ca-*.pem")
		require.NoError(t, err)
		_, _ = f.WriteString("not valid pem data")
		f.Close()
		s := SSLOpts{Mode: SSLModeVerifyCA, CA: f.Name()}
		_, err = s.TLSConfig("host")
		require.Error(t, err)
	})

	t.Run("mutual TLS cert+key loaded", func(t *testing.T) {
		certFile, keyFile := writeCommonTempCertAndKey(t)
		s := SSLOpts{Mode: SSLModeRequired, Cert: certFile, Key: keyFile}
		cfg, err := s.TLSConfig("host")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Len(t, cfg.Certificates, 1)
	})

	t.Run("cert without key returns error", func(t *testing.T) {
		certFile, _ := writeCommonTempCertAndKey(t)
		s := SSLOpts{Mode: SSLModeRequired, Cert: certFile}
		_, err := s.TLSConfig("host")
		require.Error(t, err)
	})

	t.Run("key without cert returns error", func(t *testing.T) {
		_, keyFile := writeCommonTempCertAndKey(t)
		s := SSLOpts{Mode: SSLModeRequired, Key: keyFile}
		_, err := s.TLSConfig("host")
		require.Error(t, err)
	})

	t.Run("tls_version TLSv1.2 pins MinVersion and MaxVersion", func(t *testing.T) {
		s := SSLOpts{Mode: SSLModeRequired, TLSVersion: "TLSv1.2"}
		cfg, err := s.TLSConfig("host")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
		assert.Equal(t, uint16(tls.VersionTLS12), cfg.MaxVersion)
	})

	t.Run("tls_version TLSv1.2,TLSv1.3 sets min and max", func(t *testing.T) {
		s := SSLOpts{Mode: SSLModeRequired, TLSVersion: "TLSv1.2,TLSv1.3"}
		cfg, err := s.TLSConfig("host")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
		assert.Equal(t, uint16(tls.VersionTLS13), cfg.MaxVersion)
	})

	t.Run("tls_version unsupported returns error", func(t *testing.T) {
		s := SSLOpts{Mode: SSLModeRequired, TLSVersion: "TLSv1.0"}
		_, err := s.TLSConfig("host")
		require.Error(t, err)
	})
}

func TestMergeSSLOpts(t *testing.T) {
	full := SSLOpts{
		Mode:            SSLModeVerifyCA,
		CA:              "/common/ca.pem",
		CAPath:          "/common/cadir",
		Cert:            "/common/cert.pem",
		Key:             "/common/key.pem",
		Cipher:          "AES128-SHA",
		TLSVersion:      "TLSv1.3",
		TLSCipherSuites: "TLS_AES_128_GCM_SHA256",
	}

	t.Run("empty override uses base", func(t *testing.T) {
		got := MergeSSLOpts(SSLOpts{}, full)
		assert.Equal(t, full, got)
	})

	t.Run("full override wins over base", func(t *testing.T) {
		override := SSLOpts{
			Mode:            SSLModeRequired,
			CA:              "/mysql/ca.pem",
			CAPath:          "/mysql/cadir",
			Cert:            "/mysql/cert.pem",
			Key:             "/mysql/key.pem",
			Cipher:          "AES256-SHA",
			TLSVersion:      "TLSv1.2",
			TLSCipherSuites: "TLS_AES_256_GCM_SHA384",
		}
		got := MergeSSLOpts(override, full)
		assert.Equal(t, override, got)
	})

	t.Run("partial override fills only zero fields from base", func(t *testing.T) {
		override := SSLOpts{Mode: SSLModeRequired, CA: "/mysql/ca.pem"}
		got := MergeSSLOpts(override, full)
		assert.Equal(t, SSLModeRequired, got.Mode)
		assert.Equal(t, "/mysql/ca.pem", got.CA)
		assert.Equal(t, full.CAPath, got.CAPath)
		assert.Equal(t, full.Cert, got.Cert)
		assert.Equal(t, full.TLSVersion, got.TLSVersion)
	})

	t.Run("neither argument is mutated", func(t *testing.T) {
		baseCopy := full
		override := SSLOpts{Mode: SSLModeRequired}
		_ = MergeSSLOpts(override, full)
		assert.Equal(t, baseCopy, full)
		assert.Equal(t, SSLModeRequired, override.Mode)
		assert.Empty(t, override.CA)
	})

	t.Run("both zero returns zero", func(t *testing.T) {
		got := MergeSSLOpts(SSLOpts{}, SSLOpts{})
		assert.Equal(t, SSLOpts{}, got)
	})
}

func writeCommonTempCACert(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "common-test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "ca.pem")
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: der}))
	require.NoError(t, f.Close())
	return path
}

func writeCommonTempCertAndKey(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "common-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	cf, err := os.Create(certFile)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(cf, &pem.Block{Type: "CERTIFICATE", Bytes: der}))
	require.NoError(t, cf.Close())

	keyDer, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	kf, err := os.Create(keyFile)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(kf, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer}))
	require.NoError(t, kf.Close())

	return certFile, keyFile
}
