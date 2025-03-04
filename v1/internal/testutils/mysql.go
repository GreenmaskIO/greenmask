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
	mysqlPingTries         = 100
	mysqlPingRetryInterval = 100 * time.Millisecond
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
	username      string
	Container     testcontainers.Container
	MigrationUp   string
	MigrationDown string
}

func (s *MySQLContainerSuite) SetupSuite() {
	ctx := context.Background()
	s.username = testContainerUser
	req := testcontainers.ContainerRequest{
		Image:        mysqlTestContainerImage,                 // Specify the MySQL image
		ExposedPorts: []string{mysqlTestContainerExposedPort}, // Expose the MySQL port
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": testContainerPassword,
			"MYSQL_USER":          testContainerUser,
			"MYSQL_PASSWORD":      testContainerPassword,
			"MYSQL_DATABASE":      testContainerDatabase,
		},
		//WaitingFor: wait.ForLog("ready for connections"),
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

	s.MigrateUp(ctx)
}

func (s *MySQLContainerSuite) TearDownSuite() {
	ctx := context.Background()
	s.MigrateDown(ctx)
	err := s.Container.Terminate(ctx)
	s.Assert().NoErrorf(err, "failed to terminate MySQL Container")
}

func (s *MySQLContainerSuite) SetMigrationUp(sql string) *MySQLContainerSuite {
	s.MigrationUp = sql
	return s
}

func (s *MySQLContainerSuite) SetMigrationDown(sql string) *MySQLContainerSuite {
	s.MigrationDown = sql
	return s
}

func (s *MySQLContainerSuite) GetConnection(ctx context.Context) (
	conn *sql.DB, err error,
) {
	return s.GetConnectionWithUser(ctx, testContainerUser, testContainerPassword)
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

func (s *MySQLContainerSuite) MigrateUp(ctx context.Context) {
	if s.MigrationUp == "" {
		return
	}
	conn, err := s.GetConnection(ctx)
	s.Require().NoErrorf(err, "failed to connect to MySQL")
	defer conn.Close()
	s.Require().NoErrorf(conn.Ping(), "failed to ping MySQL")
	_, err = conn.Exec(s.MigrationUp)
	s.Require().NoErrorf(err, "failed to run up migration")
}

func (s *MySQLContainerSuite) MigrateDown(ctx context.Context) {
	if s.MigrationDown == "" {
		return
	}
	conn, err := s.GetConnection(ctx)
	s.Require().NoErrorf(err, "failed to connect to MySQL")
	defer conn.Close()
	s.Require().NoErrorf(conn.Ping(), "failed to ping MySQL")
	_, err = conn.Exec(s.MigrationDown)
	s.Require().NoErrorf(err, "failed to run down migration")
}
