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

// Package ssl_e2e contains end-to-end integration tests for MySQL SSL/TLS support.
//
// Unlike the unit-level ssl_test.go (tests/integration/features/ssl/), these tests
// exercise the full dump→restore pipeline using the real mysqldump and mysql binaries.
// Each test starts a MySQL 8.4 container configured with custom TLS certificates and
// a dedicated user that requires SSL (REQUIRE SSL), then verifies that:
//
//   - Plane 1 (mysqldump/mysql CLI): SSL flags are forwarded correctly
//   - Plane 2 (go-sql-driver): DSN tls= param is set correctly
//   - Plane 3 (go-mysql-org raw client): TLS config is applied to pool connections
package ssl_e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"

	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/config"
	mysqlcmddump "github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/dump"
	mysqlcmdrestore "github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/restore"
	"github.com/greenmaskio/greenmask/pkg/storages/directory"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	mysqlImage      = "mysql:8.4"
	mysqlDatabase   = "testdb"
	sslUserName     = "ssldumpuser"
	sslUserPass     = "ssldumppass"
	containerSSLDir = "/etc/mysql/ssl"
)

const mysqlCnf = `[mysqld]
ssl-ca=/etc/mysql/ssl/ca-cert.pem
ssl-cert=/etc/mysql/ssl/server-cert.pem
ssl-key=/etc/mysql/ssl/server-key.pem
`

// ---------------------------------------------------------------------------
// TLS certificate helpers (standalone — not imported from features/ssl)
// ---------------------------------------------------------------------------

type tlsBundle struct {
	CACertPEM  []byte
	CACertFile string

	ServerCertFile string
	ServerKeyFile  string

	WrongCACertFile string
}

func generateTLSBundle(t *testing.T, dir string) *tlsBundle {
	t.Helper()

	// CA
	caKey, caCert, _, caCertPEM := generateCA(t, "e2e-test-ca")
	caCertFile := writeFile(t, filepath.Join(dir, "ca-cert.pem"), caCertPEM, 0o644)
	caKeyFile := writeFile(t, filepath.Join(dir, "ca-key.pem"), mustMarshalECKey(t, caKey), 0o600)
	_ = caKeyFile

	// Server cert signed by CA, SANs include 127.0.0.1 and localhost.
	serverKey, _, serverKeyPEM, serverCertPEM := generateSignedCert(t,
		caKey, caCert, "server",
		[]net.IP{net.ParseIP("127.0.0.1")},
		[]string{"localhost"},
	)
	_ = serverKey
	serverCertFile := writeFile(t, filepath.Join(dir, "server-cert.pem"), serverCertPEM, 0o644)
	serverKeyFile := writeFile(t, filepath.Join(dir, "server-key.pem"), serverKeyPEM, 0o600)

	// Wrong CA — not related to the server cert.
	_, _, _, wrongCACertPEM := generateCA(t, "wrong-ca")
	wrongCACertFile := writeFile(t, filepath.Join(dir, "wrong-ca-cert.pem"), wrongCACertPEM, 0o644)

	return &tlsBundle{
		CACertPEM:       caCertPEM,
		CACertFile:      caCertFile,
		ServerCertFile:  serverCertFile,
		ServerKeyFile:   serverKeyFile,
		WrongCACertFile: wrongCACertFile,
	}
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

func mustMarshalECKey(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}

func writeFile(t *testing.T, path string, data []byte, mode os.FileMode) string {
	t.Helper()
	require.NoError(t, os.WriteFile(path, data, mode))
	return path
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

type SSLSuite struct {
	testutils.MySQLContainerSuite
	host    string
	port    int
	certs   *tlsBundle
	certDir string
}

func (s *SSLSuite) SetupSuite() {
	// Require mysqldump to be in PATH — skip the whole suite if it isn't.
	if _, err := exec.LookPath("mysqldump"); err != nil {
		s.T().Skip("mysqldump not found in PATH — skipping SSL e2e tests")
	}
	if _, err := exec.LookPath("mysql"); err != nil {
		s.T().Skip("mysql not found in PATH — skipping SSL e2e tests")
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).Level(zerolog.DebugLevel)

	s.certDir = s.T().TempDir()
	s.certs = generateTLSBundle(s.T(), s.certDir)

	cnfPath := filepath.Join(s.certDir, "ssl.cnf")
	s.Require().NoError(os.WriteFile(cnfPath, []byte(mysqlCnf), 0o644))

	initSQL := fmt.Sprintf(`
CREATE USER '%s'@'%%' IDENTIFIED BY '%s' REQUIRE SSL;
GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%';
GRANT RELOAD, PROCESS, LOCK TABLES, REPLICATION CLIENT ON *.* TO '%s'@'%%';
FLUSH PRIVILEGES;
`, sslUserName, sslUserPass, mysqlDatabase, sslUserName, sslUserName)
	initSQLPath := filepath.Join(s.certDir, "init.sql")
	s.Require().NoError(os.WriteFile(initSQLPath, []byte(initSQL), 0o644))

	s.MySQLContainerSuite.
		SetImage(mysqlImage).
		SetDatabase(mysqlDatabase).
		SetScripts(initSQLPath).
		SetMigrationUp([]string{
			`CREATE TABLE IF NOT EXISTS test_table (
				id   INT PRIMARY KEY AUTO_INCREMENT,
				name VARCHAR(255) NOT NULL
			)`,
			`INSERT INTO test_table (name) VALUES ('alice'), ('bob'), ('carol')`,
		}).
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

// openRootDB opens a plain (non-SSL) connection as root for admin operations.
func (s *SSLSuite) openRootDB(ctx context.Context) *sql.DB {
	db, err := s.GetRootConnection(ctx)
	s.Require().NoError(err)
	return db
}

// seedData resets test_table to a known state using root credentials.
func (s *SSLSuite) seedData(ctx context.Context) {
	db := s.openRootDB(ctx)
	defer db.Close()

	_, err := db.ExecContext(ctx, `DELETE FROM test_table`)
	s.Require().NoError(err)

	_, err = db.ExecContext(ctx, `INSERT INTO test_table (name) VALUES ('alice'), ('bob'), ('carol')`)
	s.Require().NoError(err)
}

// countRows returns the number of rows in test_table using root credentials.
func (s *SSLSuite) countRows(ctx context.Context) int {
	db := s.openRootDB(ctx)
	defer db.Close()
	var count int
	s.Require().NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test_table").Scan(&count))
	return count
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

// baseDumpCfg returns a config.Config pre-filled with connection opts for the given user.
// It explicitly resets all fields that tests may have mutated on the shared singleton so
// that test ordering does not affect results.
func (s *SSLSuite) baseDumpCfg(user, password string) *config.Config {
	cfg := config.NewConfig()
	cfg.Engine = core.DBMSEngineMySQL
	cfg.Log.Level = "debug"
	cfg.Log.Format = "text"

	cfg.Dump.MysqlConfig.Host = s.host
	cfg.Dump.MysqlConfig.Port = s.port
	cfg.Dump.MysqlConfig.User = user
	cfg.Dump.MysqlConfig.Password = password
	cfg.Dump.MysqlConfig.ConnectDatabase = mysqlDatabase
	cfg.Dump.MysqlConfig.VendorOptions = nil
	cfg.Dump.Options.IncludeSchema = []string{mysqlDatabase}
	cfg.Dump.Options.Compression = core.CompressionNone
	cfg.Dump.Options.SchemaOnly = false
	cfg.Dump.Options.DataOnly = false
	cfg.Dump.Options.SSL = commonconfig.SSLOpts{}
	return cfg
}

// baseRestoreCfg returns a restore config matching the dump config's connection opts.
// It explicitly resets all fields that tests may have mutated on the shared singleton.
func (s *SSLSuite) baseRestoreCfg(user, password string) *config.Config {
	cfg := config.NewConfig()
	cfg.Engine = core.DBMSEngineMySQL
	cfg.Log.Level = "debug"
	cfg.Log.Format = "text"

	cfg.Restore.MysqlConfig.Host = s.host
	cfg.Restore.MysqlConfig.Port = s.port
	cfg.Restore.MysqlConfig.User = user
	cfg.Restore.MysqlConfig.Password = password
	cfg.Restore.MysqlConfig.ConnectDatabase = mysqlDatabase
	cfg.Restore.MysqlConfig.VendorOptions = nil
	cfg.Restore.Options.CreateDatabase = false
	cfg.Restore.Options.SchemaOnly = false
	cfg.Restore.Options.DataOnly = false
	cfg.Restore.Options.SSL = commonconfig.SSLOpts{}
	return cfg
}

// setupCtx wires logging and a validation collector into the context.
func setupCtx(ctx context.Context, cfg *config.Config) context.Context {
	_ = utils.SetDefaultContextLogger(cfg.Log.Level, cfg.Log.Format)
	ctx = log.Ctx(ctx).With().Str(core.MetaKeyEngine, "mysql").Logger().WithContext(ctx)
	vc := validationcollector.NewCollectorWithMeta(core.MetaKeyEngine, "mysql")
	return validationcollector.WithCollector(ctx, vc)
}

// runDump runs the dump and returns the dumpID. The dumpDir is the temp directory
// used as the root storage. It is the caller's responsibility to clean it up.
func (s *SSLSuite) runDump(ctx context.Context, cfg *config.Config, dumpDir string) (core.DumpID, error) {
	dirSt, err := directory.New(directory.NewDirectoryConfig(dumpDir))
	if err != nil {
		return "", fmt.Errorf("create directory storage: %w", err)
	}
	d, err := mysqlcmddump.NewDump(
		cfg,
		registry.DefaultTransformerRegistry,
		dirSt,
		utils.NewDefaultCmdProducer(),
		mysqlcmddump.GetMySQLDumpOpts(cfg)...,
	)
	if err != nil {
		return "", fmt.Errorf("new dump: %w", err)
	}
	if err := d.Run(ctx); err != nil {
		return "", fmt.Errorf("run dump: %w", err)
	}
	return d.GetDumpID(), nil
}

// runRestore restores from dumpDir using the given config.
func (s *SSLSuite) runRestore(ctx context.Context, cfg *config.Config, dumpDir string, dumpID core.DumpID) error {
	dirSt, err := directory.New(directory.NewDirectoryConfig(dumpDir))
	if err != nil {
		return fmt.Errorf("create directory storage: %w", err)
	}
	return mysqlcmdrestore.RunRestore(ctx, cfg, dirSt, string(dumpID))
}

// ---------------------------------------------------------------------------
// Plane-1 SSL tests: schema-only dump proves mysqldump receives correct SSL flags.
// Also covers the common SSL path (dump.options.ssl → ApplyCommon) for schema-only
// scenarios where the ApplyCommon wiring can be verified cheaply without a full cycle.
// ---------------------------------------------------------------------------

func (s *SSLSuite) TestSchemaOnlyDump() {
	cases := []struct {
		name    string
		ssl     commonconfig.SSLOpts
		wantErr bool
		// skipIf returns true when the test should be skipped at runtime.
		// Used for verify_identity which requires the host to be in the cert's SANs.
		skipIf func() bool
	}{
		{
			name: "required",
			ssl:  commonconfig.SSLOpts{Mode: commonconfig.SSLModeRequired},
		},
		{
			// DISABLED mode must fail because ssldumpuser has REQUIRE SSL on the server.
			// Proves the --ssl-mode flag is forwarded to the mysqldump subprocess.
			name:    "disabled_ssl_required_user",
			ssl:     commonconfig.SSLOpts{Mode: commonconfig.SSLModeDisabled},
			wantErr: true,
		},
		{
			name: "verify_ca_correct_ca",
			ssl:  commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyCA, CA: s.certs.CACertFile},
		},
		{
			name:    "verify_ca_wrong_ca",
			ssl:     commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyCA, CA: s.certs.WrongCACertFile},
			wantErr: true,
		},
		{
			// The server cert has 127.0.0.1 in its SANs, so connecting via 127.0.0.1 passes.
			// Skip when the container is exposed on a different address.
			name:   "verify_identity_matching_host",
			ssl:    commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyIdentity, CA: s.certs.CACertFile},
			skipIf: func() bool { return s.host != "127.0.0.1" && s.host != "localhost" },
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			if tc.skipIf != nil && tc.skipIf() {
				s.T().Skipf("host %q is not in the server cert's SANs — skipping VERIFY_IDENTITY test", s.host)
			}

			ctx := context.Background()
			dumpDir := s.T().TempDir()

			cfg := s.baseDumpCfg(sslUserName, sslUserPass)
			cfg.Dump.Options.SSL = tc.ssl
			cfg.Dump.Options.SchemaOnly = true

			ctx = setupCtx(ctx, cfg)
			_, err := s.runDump(ctx, cfg, dumpDir)
			if tc.wantErr {
				s.Require().Error(err)
			} else {
				s.Require().NoError(err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Full dump → restore cycle with SSL.
// Also covers the common SSL path (dump.options.ssl → ApplyCommon) for the
// full-cycle case where all three connection planes must receive the CA cert.
// ---------------------------------------------------------------------------

func (s *SSLSuite) TestFullCycle() {
	cases := []struct {
		name        string
		dumpSSL     commonconfig.SSLOpts
		restoreSSL  commonconfig.SSLOpts
		wantDumpErr bool
	}{
		{
			// All three planes: Plane 1 (mysqldump --ssl-mode=REQUIRED),
			// Plane 2 (tls=skip-verify in DSN), Plane 3 (InsecureSkipVerify TLS config).
			name:       "required",
			dumpSSL:    commonconfig.SSLOpts{Mode: commonconfig.SSLModeRequired},
			restoreSSL: commonconfig.SSLOpts{Mode: commonconfig.SSLModeRequired},
		},
		{
			name:       "verify_ca",
			dumpSSL:    commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyCA, CA: s.certs.CACertFile},
			restoreSSL: commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyCA, CA: s.certs.CACertFile},
		},
		{
			// Wrong CA: pool cannot establish a verified connection; dump must abort.
			name:        "verify_ca_wrong_ca_dump_fails",
			dumpSSL:     commonconfig.SSLOpts{Mode: commonconfig.SSLModeVerifyCA, CA: s.certs.WrongCACertFile},
			wantDumpErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			ctx := context.Background()
			dumpDir := s.T().TempDir()

			s.seedData(ctx)
			expectedRows := s.countRows(ctx)

			dumpCfg := s.baseDumpCfg(sslUserName, sslUserPass)
			dumpCfg.Dump.Options.SSL = tc.dumpSSL
			dumpCfg.Dump.MysqlConfig.VendorOptions = []string{"--add-drop-table"}
			dumpCtx := setupCtx(ctx, dumpCfg)

			dumpID, err := s.runDump(dumpCtx, dumpCfg, dumpDir)
			if tc.wantDumpErr {
				s.Require().Error(err)
				return
			}
			s.Require().NoError(err)

			restoreCfg := s.baseRestoreCfg(sslUserName, sslUserPass)
			restoreCfg.Restore.Options.SSL = tc.restoreSSL
			restoreCtx := setupCtx(ctx, restoreCfg)

			err = s.runRestore(restoreCtx, restoreCfg, dumpDir, dumpID)
			s.Require().NoError(err)

			s.Equal(expectedRows, s.countRows(ctx), "row count after restore must match pre-dump count")
		})
	}
}

func TestSSLSuite(t *testing.T) {
	suite.Run(t, new(SSLSuite))
}
