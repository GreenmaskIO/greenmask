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

	"github.com/docker/go-connections/nat"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/greenmaskio/greenmask/v1/internal/mysql/config"
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
	database             string
	username             string
	password             string
	migrationUser        string
	migrationPass        string
	rootUser             string
	rootPass             string
	Container            testcontainers.Container
	MigrationUp          []string
	MigrationDown        []string
	migrateAutomatically bool
	scriptPaths          []string
	image                string
}

func (s *MySQLContainerSuite) SetupSuite() {
	ctx := context.Background()
	if s.username == "" {
		s.username = testContainerUser
	}
	if s.password == "" {
		s.password = testContainerPassword
	}
	if s.migrationUser == "" {
		s.migrationUser = MysqlRootUser
	}
	if s.migrationPass == "" {
		s.migrationPass = MysqlRootPassword
	}
	if s.rootUser == "" {
		s.rootUser = MysqlRootUser
	}
	if s.rootPass == "" {
		s.rootPass = MysqlRootPassword
	}
	if s.database == "" {
		s.database = testContainerDatabase
	}
	if s.image == "" {
		s.image = mysqlTestContainerImage
	}
	var err error
	s.Container, err = tcmysql.Run(
		ctx,
		s.image,
		tcmysql.WithScripts(s.scriptPaths...),
		testcontainers.CustomizeRequestOption(
			func(req *testcontainers.GenericContainerRequest) error {
				req.Env["MYSQL_ROOT_PASSWORD"] = s.rootPass
				req.Env["MYSQL_USER"] = s.username
				req.Env["MYSQL_PASSWORD"] = s.password
				req.Env["MYSQL_DATABASE"] = s.database
				return nil
			},
		),
	)

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

//
//func (s *MySQLContainerSuite) CreateSchema(ctx context.Context, name string) {
//	conn, err := s.GetConnectionWithUser(ctx, mysqlRootUser, testContainerPassword)
//	s.Require().NoErrorf(err, "failed to connect to MySQL")
//	defer conn.Close()
//	s.Require().NoErrorf(conn.Ping(), "failed to ping MySQL")
//	_, err = conn.Exec(fmt.Sprintf("CREATE DATABASE %s", name))
//	s.Require().NoErrorf(err, "failed to create schema")
//}
//
//func (s *MySQLContainerSuite) DropSchema(name string) *MySQLContainerSuite {
//
//}

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
		username, password, host, port.Port(), testContainerDatabase,
	)
}

func (s *MySQLContainerSuite) GetConnectionWithUser(ctx context.Context, username, password string) (
	conn *sql.DB, err error,
) {
	// Create the connection string
	connStr := s.GetConnectionURIWithUser(ctx, username, password)
	print(connStr)
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
