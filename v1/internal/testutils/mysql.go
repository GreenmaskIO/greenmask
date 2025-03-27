package testutils

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/docker/go-connections/nat"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	mysqlTestContainerImage                = "mysql:8"
	mysqlTestContainerExposedPort          = "3306/tcp"
	mysqlTestContainerPort        nat.Port = "3306"
)

const (
	mysqlPingTries         = 1000
	mysqlPingRetryInterval = 100 * time.Millisecond

	mysqlRootUser     = "root"
	mysqlRootPassword = "root"
)

type readinessChecker struct{}

func (r *readinessChecker) WaitUntilReady(ctx context.Context, target wait.StrategyTarget) (err error) {
	for i := 0; i < mysqlPingTries; i++ {
		if err = r.ping(ctx, target); err == nil {
			return nil
		}
		time.Sleep(mysqlPingRetryInterval)
	}
	return fmt.Errorf("ping: %w", err)
}

func (r *readinessChecker) ping(ctx context.Context, target wait.StrategyTarget) (err error) {
	host, err := target.Host(ctx)
	if err != nil {
		return fmt.Errorf("get host: %w", err)
	}
	port, err := target.MappedPort(ctx, mysqlTestContainerPort)
	if err != nil {
		return fmt.Errorf("get port: %w", err)
	}

	// Create the connection string
	connStr := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true",
		testContainerUser, testContainerPassword, host, port.Port(), testContainerDatabase,
	)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	return nil
}

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
		s.migrationUser = testContainerUser
	}
	if s.migrationPass == "" {
		s.migrationPass = testContainerPassword
	}
	if s.rootUser == "" {
		s.rootUser = mysqlRootUser
	}
	if s.rootPass == "" {
		s.rootPass = mysqlRootPassword
	}
	if s.database == "" {
		s.database = testContainerDatabase
	}

	req := testcontainers.ContainerRequest{
		Image:        mysqlTestContainerImage,                 // Specify the MySQL image
		ExposedPorts: []string{mysqlTestContainerExposedPort}, // Expose the MySQL port
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": s.rootPass,
			"MYSQL_USER":          s.username,
			"MYSQL_PASSWORD":      s.password,
			"MYSQL_DATABASE":      s.database,
		},
		WaitingFor: &readinessChecker{},
	}

	var err error
	s.Container, err = testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		},
	)
	s.Require().NoErrorf(err, "failed to start MySQL Container")

	s.MigrateUpGlobal(ctx)
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

func (s *MySQLContainerSuite) GetConnectionWithUser(ctx context.Context, username, password string) (
	conn *sql.DB, err error,
) {
	// Get the host and port for connecting to the Container
	host, err := s.Container.Host(ctx)
	s.Require().NoErrorf(err, "failed to get Container host")
	port, err := s.Container.MappedPort(ctx, mysqlTestContainerPort)
	s.Require().NoErrorf(err, "failed to get Container port")

	// Create the connection string
	connStr := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true",
		username, password, host, port.Port(), testContainerDatabase,
	)
	print(connStr)

	return sql.Open("mysql", connStr)
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
	for _, migration := range sqls {
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
