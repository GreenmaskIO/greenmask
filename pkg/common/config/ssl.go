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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

type SSLMode string

const (
	SSLModeDisabled       SSLMode = "DISABLED"
	SSLModePreferred      SSLMode = "PREFERRED"
	SSLModeRequired       SSLMode = "REQUIRED"
	SSLModeVerifyCA       SSLMode = "VERIFY_CA"
	SSLModeVerifyIdentity SSLMode = "VERIFY_IDENTITY"
)

// SSLOpts holds TLS/SSL connection parameters that are common across all supported database engines.
// These settings apply to both the Go driver (Planes 2/3) and the external CLI tool (Plane 1).
// Fields that are forwarded only to the CLI are noted below.
//
// Field notes:
//
//   - Mode: controls both encryption and certificate verification behaviour. Accepted values:
//     DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY.
//
//   - CA: path to a PEM-encoded CA certificate file used to verify the server certificate when
//     Mode is VERIFY_CA or VERIFY_IDENTITY. Affects all three planes.
//
//   - CAPath: path to a directory containing CA certificates in PEM format (--ssl-capath).
//     Go's crypto/tls does not support directory-based CA bundles, so this field only affects
//     Plane 1 (the CLI tool). Use CA for Planes 2 and 3.
//
//   - Cert / Key: paths to the client certificate and private key for mutual TLS (mTLS). Both
//     must be set together; setting only one is an error. Affects all three planes.
//
//   - Cipher: colon-separated list of OpenSSL cipher names forwarded as --ssl-cipher to the CLI
//     (Plane 1 only), e.g. "AES128-SHA:AES256-SHA". Go's crypto/tls does not expose per-cipher
//     selection, so this field has no effect on Planes 2 and 3.
//
//   - TLSVersion: controls which TLS protocol versions are accepted. Accepted value: a
//     comma-separated subset of "TLSv1.2" and "TLSv1.3", e.g. "TLSv1.2", "TLSv1.3", or
//     "TLSv1.2,TLSv1.3". The lowest value becomes MinVersion and the highest becomes MaxVersion.
//     TLSv1.0 and TLSv1.1 are not supported. When empty, Go's default applies (TLSv1.2–TLSv1.3).
//     Forwarded as --tls-version to the CLI on Plane 1.
//
//   - TLSCipherSuites: forwarded as --tls-ciphersuites to the CLI (Plane 1 only). Only meaningful
//     for TLS 1.3 connections to MySQL. TLS 1.3 cipher suites are not configurable in Go's
//     crypto/tls, so this field has no effect on Planes 2 and 3.
type SSLOpts struct {
	Mode            SSLMode `mapstructure:"ssl_mode"         yaml:"ssl_mode"         json:"ssl_mode,omitempty"`
	CA              string  `mapstructure:"ssl_ca"           yaml:"ssl_ca"           json:"ssl_ca,omitempty"`
	CAPath          string  `mapstructure:"ssl_capath"       yaml:"ssl_capath"       json:"ssl_capath,omitempty"`
	Cert            string  `mapstructure:"ssl_cert"         yaml:"ssl_cert"         json:"ssl_cert,omitempty"`
	Key             string  `mapstructure:"ssl_key"          yaml:"ssl_key"          json:"ssl_key,omitempty"`
	Cipher          string  `mapstructure:"ssl_cipher"       yaml:"ssl_cipher"       json:"ssl_cipher,omitempty"`
	TLSVersion      string  `mapstructure:"tls_version"      yaml:"tls_version"      json:"tls_version,omitempty"`
	TLSCipherSuites string  `mapstructure:"tls_ciphersuites" yaml:"tls_ciphersuites" json:"tls_ciphersuites,omitempty"`
}

// TLSConfig builds a *tls.Config from the SSLOpts.
//
// Mapping:
//
//	DISABLED        → nil (caller sets tls=false in DSN)
//	PREFERRED       → nil (driver built-in preferred mode; raw client skips TLS)
//	REQUIRED        → InsecureSkipVerify: true
//	VERIFY_CA       → full CA verification, no hostname check
//	VERIFY_IDENTITY → full CA + hostname verification
func (s *SSLOpts) TLSConfig(serverName string) (*tls.Config, error) {
	mode := s.Mode
	if mode == "" || mode == SSLModeDisabled || mode == SSLModePreferred {
		return nil, nil
	}

	cfg := &tls.Config{}

	if s.CA != "" {
		pemData, err := os.ReadFile(s.CA)
		if err != nil {
			return nil, fmt.Errorf("read ssl_ca %q: %w", s.CA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemData) {
			return nil, fmt.Errorf("ssl_ca %q contains no valid PEM certificates", s.CA)
		}
		cfg.RootCAs = pool
	}

	switch mode {
	case SSLModeRequired:
		cfg.InsecureSkipVerify = true //nolint:gosec // intentional: REQUIRED means encrypt, no cert verification

	case SSLModeVerifyCA:
		// Go's TLS requires either ServerName or InsecureSkipVerify to be set.
		// The idiomatic pattern for "verify CA but not hostname" is:
		//   InsecureSkipVerify: true  (skip built-in hostname check)
		//   VerifyPeerCertificate: manually verify the CA chain
		cfg.InsecureSkipVerify = true //nolint:gosec // intentional: hostname NOT checked (VERIFY_CA mode)
		rootCAs := cfg.RootCAs
		cfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return verifyCAChain(rawCerts, rootCAs)
		}

	case SSLModeVerifyIdentity:
		cfg.InsecureSkipVerify = false
		if serverName != "" {
			cfg.ServerName = serverName
		}
	default:
		return nil, fmt.Errorf("unknown ssl_mode %q", mode)
	}

	if s.Cert != "" || s.Key != "" {
		if s.Cert == "" || s.Key == "" {
			return nil, fmt.Errorf("ssl_cert and ssl_key must both be set for mutual TLS")
		}
		cert, err := tls.LoadX509KeyPair(s.Cert, s.Key)
		if err != nil {
			return nil, fmt.Errorf("load ssl_cert/ssl_key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	if s.TLSVersion != "" {
		min, max, err := parseTLSVersions(s.TLSVersion)
		if err != nil {
			return nil, fmt.Errorf("parse tls_version: %w", err)
		}
		cfg.MinVersion = min
		cfg.MaxVersion = max
	}

	return cfg, nil
}

// MergeSSLOpts returns a new SSLOpts where every zero field in override is filled
// from base. Non-zero fields in override are preserved unchanged.
// This is a pure function — neither argument is mutated.
func MergeSSLOpts(override, base SSLOpts) SSLOpts {
	merged := override
	if merged.Mode == "" {
		merged.Mode = base.Mode
	}
	if merged.CA == "" {
		merged.CA = base.CA
	}
	if merged.CAPath == "" {
		merged.CAPath = base.CAPath
	}
	if merged.Cert == "" {
		merged.Cert = base.Cert
	}
	if merged.Key == "" {
		merged.Key = base.Key
	}
	if merged.Cipher == "" {
		merged.Cipher = base.Cipher
	}
	if merged.TLSVersion == "" {
		merged.TLSVersion = base.TLSVersion
	}
	if merged.TLSCipherSuites == "" {
		merged.TLSCipherSuites = base.TLSCipherSuites
	}
	return merged
}

// verifyCAChain manually verifies the peer certificate chain against the given root pool.
// rawCerts contains DER-encoded certificates from the server (leaf first, then intermediates).
func verifyCAChain(rawCerts [][]byte, roots *x509.CertPool) error {
	if len(rawCerts) == 0 {
		return fmt.Errorf("no peer certificates presented")
	}
	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for _, raw := range rawCerts {
		c, err := x509.ParseCertificate(raw)
		if err != nil {
			return fmt.Errorf("parse peer certificate: %w", err)
		}
		certs = append(certs, c)
	}
	intermediates := x509.NewCertPool()
	for _, c := range certs[1:] {
		intermediates.AddCert(c)
	}
	opts := x509.VerifyOptions{
		Roots:         roots, // nil means use system roots
		Intermediates: intermediates,
	}
	if _, err := certs[0].Verify(opts); err != nil {
		return fmt.Errorf("certificate chain verification failed: %w", err)
	}
	return nil
}

// parseTLSVersions converts "TLSv1.2,TLSv1.3" into Go tls version constants.
func parseTLSVersions(spec string) (minVer, maxVer uint16, err error) {
	var parsed []uint16
	for v := range strings.SplitSeq(spec, ",") {
		switch strings.TrimSpace(v) {
		case "TLSv1.2":
			parsed = append(parsed, tls.VersionTLS12)
		case "TLSv1.3":
			parsed = append(parsed, tls.VersionTLS13)
		default:
			return 0, 0, fmt.Errorf("unsupported TLS version %q (supported: TLSv1.2, TLSv1.3)", strings.TrimSpace(v))
		}
	}
	if len(parsed) == 0 {
		return 0, 0, fmt.Errorf("no TLS versions parsed from %q", spec)
	}
	lo, hi := parsed[0], parsed[0]
	for _, ver := range parsed[1:] {
		if ver < lo {
			lo = ver
		}
		if ver > hi {
			hi = ver
		}
	}
	return lo, hi, nil
}
