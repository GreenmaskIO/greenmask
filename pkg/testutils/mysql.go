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

package testutils

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/docker/go-connections/nat"
	_ "github.com/go-sql-driver/mysql"
	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/config"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

var (
	mysqlTestContainerImage          = "mysql:8"
	mysqlTestContainerPort  nat.Port = "3306"
)

const (
	MysqlRootUser     = "root"
	MysqlRootPassword = testContainerPassword
)

type MySQLContainerSuite struct {
	suite.Suite
	database      string
	username      string
	password      string
	migrationUser string
	migrationPass string
	rootUser      string
	rootPass      string
	Container     testcontainers.Container
	MigrationUp   []string
	MigrationDown []string
	scriptPaths   []string
	image         string
	containerOpts []testcontainers.ContainerCustomizer
}

func (s *MySQLContainerSuite) SetupSuite() {
	ctx := context.Background()
	if s.username == "" {
		s.username = testContainerUser
	}
	if s.password == "" {
		s.password = testContainerPassword
	}
	if s.rootUser == "" {
		s.rootUser = MysqlRootUser
	}
	if s.rootPass == "" {
		s.rootPass = MysqlRootPassword
	}
	if s.migrationUser == "" {
		s.migrationUser = MysqlRootUser
	}
	// Default migration password matches the root password so that migrations can
	// connect via root@% (created by the built-in init script below).
	if s.migrationPass == "" {
		s.migrationPass = s.rootPass
	}
	if s.database == "" {
		s.database = testContainerDatabase
	}
	if s.image == "" {
		s.image = mysqlTestContainerImage
	}

	// tcmysql.Run always appends WithDefaultCredentials() last, which sets
	// MYSQL_ROOT_PASSWORD = MYSQL_PASSWORD.  That means any MYSQL_ROOT_PASSWORD we
	// set in an env customizer gets overridden.  The resulting root@localhost account
	// therefore carries the regular user's password, not s.rootPass.
	//
	// To give tests a consistent root account that is reachable from outside the
	// container (published port), we inject a built-in init script that creates
	// root@% with s.rootPass.  The script is prepended so it runs before any
	// caller-supplied scripts.
	rootGrantSQL := fmt.Sprintf(
		"CREATE USER IF NOT EXISTS 'root'@'%%' IDENTIFIED BY '%s';\n"+
			"GRANT ALL PRIVILEGES ON *.* TO 'root'@'%%' WITH GRANT OPTION;\n"+
			"FLUSH PRIVILEGES;\n",
		s.rootPass,
	)
	rootGrantFile, err := os.CreateTemp("", "greenmask-mysql-root-*.sql")
	s.Require().NoError(err)
	_, err = rootGrantFile.WriteString(rootGrantSQL)
	s.Require().NoError(err)
	s.Require().NoError(rootGrantFile.Close())
	// The file is copied into the container by testcontainers before tcmysql.Run
	// returns, so it is safe to remove it afterwards.
	defer os.Remove(rootGrantFile.Name())

	allScripts := append([]string{rootGrantFile.Name()}, s.scriptPaths...)

	runOpts := []testcontainers.ContainerCustomizer{
		tcmysql.WithScripts(allScripts...),
		testcontainers.CustomizeRequestOption(
			func(req *testcontainers.GenericContainerRequest) error {
				req.Env["MYSQL_ROOT_PASSWORD"] = s.rootPass
				req.Env["MYSQL_ROOT_HOST"] = "%"
				req.Env["MYSQL_USER"] = s.username
				req.Env["MYSQL_PASSWORD"] = s.password
				req.Env["MYSQL_DATABASE"] = s.database
				return nil
			},
		),
	}
	runOpts = append(runOpts, s.containerOpts...)
	s.Container, err = tcmysql.Run(ctx, s.image, runOpts...)

	s.Require().NoErrorf(err, "failed to start MySQL Container")

	s.MigrateUpGlobal(ctx)
}

func (s *MySQLContainerSuite) SetImage(image string) *MySQLContainerSuite {
	s.image = image
	return s
}

func (s *MySQLContainerSuite) TearDownSuite() {
	ctx := context.Background()
	s.MigrateDownGlobal(ctx)
	err := s.Container.Terminate(ctx)
	s.Assert().NoErrorf(err, "failed to terminate MySQL Container")
}

func (s *MySQLContainerSuite) SetMigrationUser(userName, password string) *MySQLContainerSuite {
	s.migrationUser = userName
	s.migrationPass = password
	return s
}

func (s *MySQLContainerSuite) SetUser(userName, password string) *MySQLContainerSuite {
	s.username = userName
	s.password = password
	return s
}

func (s *MySQLContainerSuite) SetMigrationUp(sqls []string) *MySQLContainerSuite {
	s.MigrationUp = sqls
	return s
}

func (s *MySQLContainerSuite) SetMigrationDown(sqls []string) *MySQLContainerSuite {
	s.MigrationDown = sqls
	return s
}

func (s *MySQLContainerSuite) SetDatabase(name string) *MySQLContainerSuite {
	s.database = name
	return s
}

func (s *MySQLContainerSuite) SetRootUser(userName, password string) *MySQLContainerSuite {
	s.rootUser = userName
	s.rootPass = password
	return s
}

func (s *MySQLContainerSuite) SetScripts(scripts ...string) *MySQLContainerSuite {
	s.scriptPaths = scripts
	return s
}

// SetContainerOptions appends extra testcontainers customizers that are applied
// when the MySQL container is started. Use this to mount files (e.g. TLS
// certificates, custom my.cnf snippets) or set additional environment variables.
func (s *MySQLContainerSuite) SetContainerOptions(opts ...testcontainers.ContainerCustomizer) *MySQLContainerSuite {
	s.containerOpts = append(s.containerOpts, opts...)
	return s
}

func (s *MySQLContainerSuite) GetConnection(ctx context.Context) (
	conn *sql.DB, err error,
) {
	return s.GetConnectionWithUser(ctx, s.username, s.password)
}

func (s *MySQLContainerSuite) GetRootConnection(ctx context.Context) (
	conn *sql.DB, err error,
) {
	return s.GetConnectionWithUser(ctx, s.rootUser, s.rootPass)
}

func (s *MySQLContainerSuite) GetConnectionOpts(ctx context.Context) config.ConnectionOpts {
	return s.GetConnectionOptsWithUser(ctx, s.username, s.password)
}

func (s *MySQLContainerSuite) GetRootConnectionOpts(ctx context.Context) config.ConnectionOpts {
	return s.GetConnectionOptsWithUser(ctx, s.rootUser, s.rootPass)
}

func (s *MySQLContainerSuite) GetRootConnConfig(ctx context.Context) *mysqlmodels.ConnConfig {
	opts := s.GetRootConnectionOpts(ctx)
	cfg, err := opts.ConnectionConfig(commonconfig.SSLOpts{})
	s.Require().NoError(err, "failed to build ConnConfig for root user")
	return cfg
}

func (s *MySQLContainerSuite) GetConnectionOptsWithUser(ctx context.Context, username, password string) config.ConnectionOpts {
	// Get the host and port for connecting to the Container
	host, err := s.Container.Host(ctx)
	s.Require().NoErrorf(err, "failed to get Container host")
	port, err := s.Container.MappedPort(ctx, mysqlTestContainerPort)
	s.Require().NoErrorf(err, "failed to get Container port")
	return config.ConnectionOpts{
		Host:     host,
		Port:     port.Int(),
		User:     username,
		Password: password,
	}
}

func (s *MySQLContainerSuite) GetConnectionURI(ctx context.Context) string {
	return s.GetConnectionURIWithUser(ctx, s.username, s.password)
}

func (s *MySQLContainerSuite) GetConnectionURIWithUser(ctx context.Context, username, password string) string {
	// Get the host and port for connecting to the Container
	host, err := s.Container.Host(ctx)
	s.Require().NoErrorf(err, "failed to get Container host")
	port, err := s.Container.MappedPort(ctx, mysqlTestContainerPort)
	s.Require().NoErrorf(err, "failed to get Container port")
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true",
		username, password, host, port.Port(), s.database,
	)
}

func (s *MySQLContainerSuite) GetConnectionWithUser(ctx context.Context, username, password string) (
	conn *sql.DB, err error,
) {
	connStr := s.GetConnectionURIWithUser(ctx, username, password)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, fmt.Errorf("open mysql connection: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	return db, nil
}

func (s *MySQLContainerSuite) MigrateUpGlobal(ctx context.Context) {
	s.MigrateUp(ctx, s.MigrationUp)
}

func (s *MySQLContainerSuite) MigrateUp(ctx context.Context, sqls []string) {
	if len(sqls) == 0 {
		return
	}
	conn, err := s.GetConnectionWithUser(ctx, s.migrationUser, s.migrationPass)
	s.Require().NoErrorf(err, "failed to connect to MySQL")
	defer conn.Close()
	s.Require().NoErrorf(conn.Ping(), "failed to ping MySQL")
	for i, migration := range sqls {
		log.Info().
			Str("migration", migration).
			Int("index", i).
			Msg("running migration")
		_, err = conn.Exec(migration)
		s.Require().NoErrorf(err, "failed to run up migration")
	}
	s.Require().NoErrorf(err, "failed to run up migration")
}

func (s *MySQLContainerSuite) MigrateDownGlobal(ctx context.Context) {
	s.MigrateDown(ctx, s.MigrationDown)
}

func (s *MySQLContainerSuite) MigrateDown(ctx context.Context, sqls []string) {
	if len(sqls) == 0 {
		return
	}
	conn, err := s.GetConnectionWithUser(ctx, s.migrationUser, s.migrationPass)
	s.Require().NoErrorf(err, "failed to connect to MySQL")
	defer conn.Close()
	s.Require().NoErrorf(conn.Ping(), "failed to ping MySQL")
	for _, migration := range sqls {
		_, err = conn.Exec(migration)
		s.Require().NoErrorf(err, "failed to run down migration")
	}
	s.Require().NoErrorf(err, "failed to run down migration")
}
