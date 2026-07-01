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

// Package ssl contains integration tests for MySQL SSL/TLS connection support.
// Each test requires a real MySQL container started with custom TLS certificates
// generated at test time. All three connection planes are exercised:
//
//   - Plane 1 (CLI): verified via unit tests on ConnectionOpts.Params()
//   - Plane 2 (go-sql-driver): sql.Open with DSN from ConnConfig.URI()
//   - Plane 3 (go-mysql-org): ConsistentTxPool.Init() using ConnConfig.TLSConfig
package ssl

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"

	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/greenmaskio/greenmask/pkg/mysql/pool"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

// ---------------------------------------------------------------------------
// TLS certificate generation helpers
// ---------------------------------------------------------------------------

type tlsBundle struct {
	// CA that signed the server certificate.
	CACert     *x509.Certificate
	CAKey      *ecdsa.PrivateKey
	CACertPEM  []byte
	CAKeyPEM   []byte
	CACertFile string
	CAKeyFile  string

	// Server certificate (CN=localhost, SANs=[127.0.0.1, localhost]).
	ServerCert     *x509.Certificate
	ServerKey      *ecdsa.PrivateKey
	ServerCertPEM  []byte
	ServerKeyPEM   []byte
	ServerCertFile string
	ServerKeyFile  string

	// Client certificate — used for mutual TLS.
	ClientCert     *x509.Certificate
	ClientKey      *ecdsa.PrivateKey
	ClientCertPEM  []byte
	ClientKeyPEM   []byte
	ClientCertFile string
	ClientKeyFile  string

	// WrongCACertFile is a self-signed CA that did NOT sign the server cert.
	// Connecting with VERIFY_CA against this CA must fail.
	WrongCACertFile string
	WrongCACertPEM  []byte
}

func generateTLSBundle(t *testing.T, dir string) *tlsBundle {
	t.Helper()
	b := &tlsBundle{}

	// CA
	b.CAKey, b.CACert, b.CAKeyPEM, b.CACertPEM = generateCA(t, "test-ca")
	b.CACertFile = writeFile(t, filepath.Join(dir, "ca-cert.pem"), b.CACertPEM, 0o644)
	b.CAKeyFile = writeFile(t, filepath.Join(dir, "ca-key.pem"), b.CAKeyPEM, 0o600)

	// Server cert signed by CA, SANs include 127.0.0.1 and localhost.
	b.ServerKey, b.ServerCert, b.ServerKeyPEM, b.ServerCertPEM = generateSignedCert(t,
		b.CAKey, b.CACert,
		"server",
		[]net.IP{net.ParseIP("127.0.0.1")},
		[]string{"localhost"},
	)
	b.ServerCertFile = writeFile(t, filepath.Join(dir, "server-cert.pem"), b.ServerCertPEM, 0o644)
	b.ServerKeyFile = writeFile(t, filepath.Join(dir, "server-key.pem"), b.ServerKeyPEM, 0o600)

	// Client cert signed by CA.
	b.ClientKey, b.ClientCert, b.ClientKeyPEM, b.ClientCertPEM = generateSignedCert(t,
		b.CAKey, b.CACert,
		"client",
		nil, nil,
	)
	b.ClientCertFile = writeFile(t, filepath.Join(dir, "client-cert.pem"), b.ClientCertPEM, 0o644)
	b.ClientKeyFile = writeFile(t, filepath.Join(dir, "client-key.pem"), b.ClientKeyPEM, 0o600)

	// Wrong CA (self-signed, not related to the server cert).
	wrongKey, wrongCert, wrongKeyPEM, wrongCertPEM := generateCA(t, "wrong-ca")
	_ = wrongKey
	_ = wrongCert
	_ = wrongKeyPEM
	b.WrongCACertPEM = wrongCertPEM
	b.WrongCACertFile = writeFile(t, filepath.Join(dir, "wrong-ca-cert.pem"), wrongCertPEM, 0o644)

	return b
}

func generateCA(t *testing.T, cn string) (*ecdsa.PrivateKey, *x509.Certificate, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDer, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer})
	return key, cert, keyPEM, certPEM
}

func generateSignedCert(
	t *testing.T,
	caKey *ecdsa.PrivateKey,
	caCert *x509.Certificate,
	cn string,
	ips []net.IP,
	dns []string,
) (*ecdsa.PrivateKey, *x509.Certificate, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  ips,
		DNSNames:     dns,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDer, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer})
	return key, cert, keyPEM, certPEM
}

func writeFile(t *testing.T, path string, data []byte, mode os.FileMode) string {
	t.Helper()
	require.NoError(t, os.WriteFile(path, data, mode))
	return path
}

// ---------------------------------------------------------------------------
// SSL container suite
// ---------------------------------------------------------------------------

const (
	mysqlImage      = "mysql:8.4"
	mysqlDatabase   = "testdb"
	sslUserName     = "ssluser"
	sslUserPass     = "sslpass"
	mtlsUserName    = "mtlsuser"
	mtlsUserPass    = "mtlspass"
	containerSSLDir = "/etc/mysql/ssl"
)

// mysqlRootPass is the root password used by the test container.
// It matches testutils.MysqlRootPassword so the testcontainers init
// process (which always overrides MYSQL_ROOT_PASSWORD with MYSQL_PASSWORD)
// sets the same value we expect to authenticate with.
var mysqlRootPass = testutils.MysqlRootPassword

// mysqlCnf is the [mysqld] config that activates our custom SSL certs.
// MySQL 8 also generates its own certs in /var/lib/mysql; this file overrides them.
const mysqlCnf = `[mysqld]
ssl-ca=/etc/mysql/ssl/ca-cert.pem
ssl-cert=/etc/mysql/ssl/server-cert.pem
ssl-key=/etc/mysql/ssl/server-key.pem
`

type SSLSuite struct {
	testutils.MySQLContainerSuite
	host    string
	port    int
	certs   *tlsBundle
	certDir string
}

func (s *SSLSuite) SetupSuite() {
	s.certDir = s.T().TempDir()
	s.certs = generateTLSBundle(s.T(), s.certDir)

	cnfPath := filepath.Join(s.certDir, "custom.cnf")
	s.Require().NoError(os.WriteFile(cnfPath, []byte(mysqlCnf), 0o644))

	initSQL := fmt.Sprintf(`
CREATE USER '%s'@'%%' IDENTIFIED BY '%s';
GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%';

CREATE USER '%s'@'%%' IDENTIFIED BY '%s' REQUIRE X509;
GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%';

FLUSH PRIVILEGES;
`, sslUserName, sslUserPass, mysqlDatabase, sslUserName,
		mtlsUserName, mtlsUserPass, mysqlDatabase, mtlsUserName)

	initSQLPath := filepath.Join(s.certDir, "init.sql")
	s.Require().NoError(os.WriteFile(initSQLPath, []byte(initSQL), 0o644))

	s.MySQLContainerSuite.
		SetImage(mysqlImage).
		SetDatabase(mysqlDatabase).
		SetScripts(initSQLPath).
		SetContainerOptions(
			testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
				req.Files = append(req.Files,
					testcontainers.ContainerFile{
						HostFilePath:      cnfPath,
						ContainerFilePath: "/etc/mysql/conf.d/ssl.cnf",
						FileMode:          0o644,
					},
					testcontainers.ContainerFile{
						HostFilePath:      s.certs.CACertFile,
						ContainerFilePath: containerSSLDir + "/ca-cert.pem",
						FileMode:          0o644,
					},
					testcontainers.ContainerFile{
						HostFilePath:      s.certs.ServerCertFile,
						ContainerFilePath: containerSSLDir + "/server-cert.pem",
						FileMode:          0o644,
					},
					testcontainers.ContainerFile{
						HostFilePath:      s.certs.ServerKeyFile,
						ContainerFilePath: containerSSLDir + "/server-key.pem",
						FileMode:          0o644,
					},
				)
				return nil
			}),
		)

	s.MySQLContainerSuite.SetupSuite()

	ctx := context.Background()
	opts := s.GetRootConnectionOpts(ctx)
	s.host = opts.Host
	s.port = opts.Port
}

// ---------------------------------------------------------------------------
// Helper: open a *sql.DB via ConnConfig.URI()
// ---------------------------------------------------------------------------

func (s *SSLSuite) openDB(cfg *mysqlmodels.ConnConfig) (*sql.DB, error) {
	uri, err := cfg.URI()
	if err != nil {
		return nil, err
	}
	return sql.Open("mysql", uri)
}

// querySslCipher returns the current connection's Ssl_cipher status variable.
// An empty string means TLS was not used.
func querySslCipher(ctx context.Context, db *sql.DB) (string, error) {
	var name, value string
	row := db.QueryRowContext(ctx, "SHOW STATUS LIKE 'Ssl_cipher'")
	if err := row.Scan(&name, &value); err != nil {
		return "", err
	}
	return value, nil
}

// rootConfig builds a base ConnConfig for the root user.
func (s *SSLSuite) rootConfig() *mysqlmodels.ConnConfig {
	return &mysqlmodels.ConnConfig{
		Host:     s.host,
		Port:     s.port,
		User:     "root",
		Password: mysqlRootPass,
		Database: mysqlDatabase,
	}
}

// ---------------------------------------------------------------------------
// Plane-2 tests (go-sql-driver via ConnConfig.URI)
// ---------------------------------------------------------------------------

func (s *SSLSuite) TestPlane2() {
	cases := []struct {
		name        string
		buildCfg    func() *mysqlmodels.ConnConfig
		wantPingErr bool
		checkCipher bool
		wantTLS     bool // only meaningful when checkCipher is true
	}{
		{
			name: "disabled_no_tls",
			buildCfg: func() *mysqlmodels.ConnConfig {
				c := s.rootConfig()
				c.SSLMode = commonconfig.SSLModeDisabled
				return c
			},
			checkCipher: true,
			wantTLS:     false,
		},
		{
			name: "required_tls_active",
			buildCfg: func() *mysqlmodels.ConnConfig {
				c := s.rootConfig()
				c.SSLMode = commonconfig.SSLModeRequired
				return c
			},
			checkCipher: true,
			wantTLS:     true,
		},
		{
			name: "preferred_connects_successfully",
			buildCfg: func() *mysqlmodels.ConnConfig {
				c := s.rootConfig()
				c.SSLMode = commonconfig.SSLModePreferred
				return c
			},
		},
		{
			name: "verify_ca_correct_ca",
			buildCfg: func() *mysqlmodels.ConnConfig {
				opts := commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyCA, CA: s.certs.CACertFile}
				tlsCfg, err := opts.TLSConfig(s.host)
				s.Require().NoError(err)
				c := s.rootConfig()
				c.SSLMode = commonconfig.SSLModeVerifyCA
				c.TLSConfig = tlsCfg
				return c
			},
			checkCipher: true,
			wantTLS:     true,
		},
		{
			name: "verify_ca_wrong_ca",
			buildCfg: func() *mysqlmodels.ConnConfig {
				opts := commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyCA, CA: s.certs.WrongCACertFile}
				tlsCfg, err := opts.TLSConfig(s.host)
				s.Require().NoError(err)
				c := s.rootConfig()
				c.SSLMode = commonconfig.SSLModeVerifyCA
				c.TLSConfig = tlsCfg
				return c
			},
			wantPingErr: true,
		},
		{
			name: "verify_identity_matching_hostname",
			buildCfg: func() *mysqlmodels.ConnConfig {
				certPool := x509.NewCertPool()
				s.Require().True(certPool.AppendCertsFromPEM(s.certs.CACertPEM))
				c := s.rootConfig()
				c.SSLMode = commonconfig.SSLModeVerifyIdentity
				c.TLSConfig = &tls.Config{RootCAs: certPool, ServerName: s.host}
				return c
			},
			checkCipher: true,
			wantTLS:     true,
		},
		{
			name: "verify_identity_wrong_hostname",
			buildCfg: func() *mysqlmodels.ConnConfig {
				certPool := x509.NewCertPool()
				s.Require().True(certPool.AppendCertsFromPEM(s.certs.CACertPEM))
				c := s.rootConfig()
				c.SSLMode = commonconfig.SSLModeVerifyIdentity
				c.TLSConfig = &tls.Config{RootCAs: certPool, ServerName: "wrong.hostname.example.com"}
				return c
			},
			wantPingErr: true,
		},
		{
			name: "mutual_tls_with_client_cert",
			buildCfg: func() *mysqlmodels.ConnConfig {
				caPool := x509.NewCertPool()
				s.Require().True(caPool.AppendCertsFromPEM(s.certs.CACertPEM))
				clientCert, err := tls.X509KeyPair(s.certs.ClientCertPEM, s.certs.ClientKeyPEM)
				s.Require().NoError(err)
				return &mysqlmodels.ConnConfig{
					Host:     s.host,
					Port:     s.port,
					User:     mtlsUserName,
					Password: mtlsUserPass,
					Database: mysqlDatabase,
					SSLMode:  commonconfig.SSLModeVerifyCA,
					TLSConfig: &tls.Config{
						RootCAs:      caPool,
						Certificates: []tls.Certificate{clientCert},
					},
				}
			},
		},
		{
			name: "mutual_tls_without_client_cert",
			buildCfg: func() *mysqlmodels.ConnConfig {
				return &mysqlmodels.ConnConfig{
					Host:     s.host,
					Port:     s.port,
					User:     mtlsUserName,
					Password: mtlsUserPass,
					Database: mysqlDatabase,
					SSLMode:  commonconfig.SSLModeRequired,
				}
			},
			wantPingErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			ctx := context.Background()
			db, err := s.openDB(tc.buildCfg())
			s.Require().NoError(err)
			defer db.Close()

			err = db.PingContext(ctx)
			if tc.wantPingErr {
				s.Require().Error(err)
				return
			}
			s.Require().NoError(err)

			if tc.checkCipher {
				cipher, err := querySslCipher(ctx, db)
				s.Require().NoError(err)
				if tc.wantTLS {
					s.NotEmpty(cipher, "expected TLS cipher to be non-empty")
				} else {
					s.Empty(cipher, "expected no TLS (cipher must be empty)")
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Plane-3 tests (go-mysql-org raw client via ConsistentTxPool)
// ---------------------------------------------------------------------------

func (s *SSLSuite) TestPlane3() {
	cases := []struct {
		name        string
		buildCfg    func() *mysqlmodels.ConnConfig
		wantInitErr bool
		checkCipher bool
		wantTLS     bool // only meaningful when checkCipher is true
	}{
		{
			name: "required_tls_active",
			buildCfg: func() *mysqlmodels.ConnConfig {
				c := s.rootConfig()
				c.SSLMode = commonconfig.SSLModeRequired
				c.TLSConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
				return c
			},
			checkCipher: true,
			wantTLS:     true,
		},
		{
			name: "verify_ca_correct_ca",
			buildCfg: func() *mysqlmodels.ConnConfig {
				opts := commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyCA, CA: s.certs.CACertFile}
				tlsCfg, err := opts.TLSConfig(s.host)
				s.Require().NoError(err)
				c := s.rootConfig()
				c.SSLMode = commonconfig.SSLModeVerifyCA
				c.TLSConfig = tlsCfg
				return c
			},
		},
		{
			name: "verify_ca_wrong_ca",
			buildCfg: func() *mysqlmodels.ConnConfig {
				opts := commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyCA, CA: s.certs.WrongCACertFile}
				tlsCfg, err := opts.TLSConfig(s.host)
				s.Require().NoError(err)
				c := s.rootConfig()
				c.SSLMode = commonconfig.SSLModeVerifyCA
				c.TLSConfig = tlsCfg
				return c
			},
			wantInitErr: true,
		},
		{
			name: "disabled_no_tls",
			buildCfg: func() *mysqlmodels.ConnConfig {
				return s.rootConfig() // no TLSConfig → raw client connects without TLS
			},
			checkCipher: true,
			wantTLS:     false,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			p := pool.NewConsistentTxPool(tc.buildCfg(), 1)
			err := p.Init(ctx)
			if tc.wantInitErr {
				s.Require().Error(err)
				p.Close(ctx)
				return
			}
			s.Require().NoError(err)
			defer p.Close(ctx)

			if tc.checkCipher {
				err := p.RunWithConn(ctx, func(_ context.Context, worker pool.WorkerConn) error {
					rows, err := worker.RawConn().Execute("SHOW STATUS LIKE 'Ssl_cipher'")
					s.Require().NoError(err)
					s.Require().NotNil(rows.Resultset)
					s.Require().Equal(1, rows.RowNumber())
					cipher, err := rows.GetString(0, 1)
					s.Require().NoError(err)
					if tc.wantTLS {
						s.NotEmpty(cipher, "expected TLS cipher to be non-empty for raw connection")
					} else {
						s.Empty(cipher, "expected no TLS for raw connection without TLSConfig")
					}
					return nil
				})
				s.Require().NoError(err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SSLOpts integration — cert file validation and full connection flow
// ---------------------------------------------------------------------------

func (s *SSLSuite) TestSSLOpts() {
	cases := []struct {
		name        string
		buildOpts   func() commonconfig.SSLOpts
		wantOptsErr bool
		wantCertLen int // if > 0, assert len(tlsCfg.Certificates)
		buildCfg    func(tlsCfg *tls.Config) *mysqlmodels.ConnConfig
		checkCipher bool
	}{
		{
			name: "missing_ca_file_returns_error",
			buildOpts: func() commonconfig.SSLOpts {
				return commonconfig.SSLOpts{
					Mode: commonconfig.SSLModeVerifyCA,
					CA:   "/nonexistent/path/ca.pem",
				}
			},
			wantOptsErr: true,
		},
		{
			name: "missing_key_file_returns_error",
			buildOpts: func() commonconfig.SSLOpts {
				return commonconfig.SSLOpts{
					Mode: commonconfig.SSLModeRequired,
					Cert: s.certs.ClientCertFile,
					Key:  "/nonexistent/path/key.pem",
				}
			},
			wantOptsErr: true,
		},
		{
			name: "full_flow_verify_ca",
			buildOpts: func() commonconfig.SSLOpts {
				return commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyCA, CA: s.certs.CACertFile}
			},
			buildCfg: func(tlsCfg *tls.Config) *mysqlmodels.ConnConfig {
				c := s.rootConfig()
				c.SSLMode = commonconfig.SSLModeVerifyCA
				c.TLSConfig = tlsCfg
				return c
			},
			checkCipher: true,
		},
		{
			name: "full_flow_mutual_tls",
			buildOpts: func() commonconfig.SSLOpts {
				return commonconfig.SSLOpts{
					Mode: commonconfig.SSLModeVerifyCA,
					CA:   s.certs.CACertFile,
					Cert: s.certs.ClientCertFile,
					Key:  s.certs.ClientKeyFile,
				}
			},
			wantCertLen: 1,
			buildCfg: func(tlsCfg *tls.Config) *mysqlmodels.ConnConfig {
				return &mysqlmodels.ConnConfig{
					Host:      s.host,
					Port:      s.port,
					User:      mtlsUserName,
					Password:  mtlsUserPass,
					Database:  mysqlDatabase,
					SSLMode:   commonconfig.SSLModeVerifyCA,
					TLSConfig: tlsCfg,
				}
			},
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			ctx := context.Background()

			o := tc.buildOpts()
			tlsCfg, err := o.TLSConfig(s.host)
			if tc.wantOptsErr {
				s.Require().Error(err)
				return
			}
			s.Require().NoError(err)
			s.Require().NotNil(tlsCfg)

			if tc.wantCertLen > 0 {
				s.Require().Len(tlsCfg.Certificates, tc.wantCertLen)
			}

			db, err := s.openDB(tc.buildCfg(tlsCfg))
			s.Require().NoError(err)
			defer db.Close()
			s.Require().NoError(db.PingContext(ctx))

			if tc.checkCipher {
				cipher, err := querySslCipher(ctx, db)
				s.Require().NoError(err)
				s.NotEmpty(cipher)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------------------

func TestSSLSuite(t *testing.T) {
	suite.Run(t, new(SSLSuite))
}
