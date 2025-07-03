package testutils

import (
	"context"
	"fmt"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	testContainerDatabase = "testdb"
	testContainerUser     = "testuser"
	testContainerPassword = "testpassword"
)

const (
	pgTestContainerPort        nat.Port = "5432"
	pgTestContainerImage                = "postgres:17"
	pgTestContainerExposedPort          = "5432/tcp"
)

type PgContainerSuite struct {
	suite.Suite
	username      string
	Container     testcontainers.Container
	MigrationUp   string
	MigrationDown string
}

func (s *PgContainerSuite) SetupSuite() {
	ctx := context.Background()
	s.username = testContainerUser
	req := testcontainers.ContainerRequest{
		Image:        pgTestContainerImage,                 // Specify the PostgreSQL image
		ExposedPorts: []string{pgTestContainerExposedPort}, // Expose the PostgreSQL port
		Env: map[string]string{
			"POSTGRES_USER":     testContainerUser,
			"POSTGRES_PASSWORD": testContainerPassword,
			"POSTGRES_DB":       testContainerDatabase,
		},
		WaitingFor: wait.ForSQL(pgTestContainerExposedPort, "pgx", func(host string, port nat.Port) string {
			return fmt.Sprintf(
				"postgres://%s:%s@%s:%s/%s?sslmode=disable",
				testContainerUser, testContainerPassword, host, port.Port(), testContainerDatabase,
			)
		}),
	}

	var err error
	s.Container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	s.Require().NoErrorf(err, "failed to start PostgreSQL Container")

	s.MigrateUp(ctx)
}

func (s *PgContainerSuite) TearDownSuite() {
	ctx := context.Background()
	s.MigrateDown(ctx)
	err := s.Container.Terminate(ctx)
	s.Assert().NoErrorf(err, "failed to terminate PostgreSQL Container")
}

func (s *PgContainerSuite) SetMigrationUp(sql string) *PgContainerSuite {
	s.MigrationUp = sql
	return s
}

func (s *PgContainerSuite) SetMigrationDown(sql string) *PgContainerSuite {
	s.MigrationDown = sql
	return s
}

func (s *PgContainerSuite) GetConnection(ctx context.Context) (
	conn *pgx.Conn, err error,
) {
	return s.GetConnectionWithUser(ctx, testContainerUser, testContainerPassword)
}

func (s *PgContainerSuite) GetConnectionWithUser(ctx context.Context, username, password string) (
	conn *pgx.Conn, err error,
) {
	// Get the host and port for connecting to the Container
	host, err := s.Container.Host(ctx)
	s.Require().NoErrorf(err, "failed to get Container host")
	port, err := s.Container.MappedPort(ctx, pgTestContainerPort)
	s.Require().NoErrorf(err, "failed to get Container port")

	// Create the connection string
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		username, password, host, port.Port(), testContainerDatabase,
	)

	return pgx.Connect(ctx, connStr)
}

func (s *PgContainerSuite) GetSuperUser() string {
	return testContainerUser
}

func (s *PgContainerSuite) MigrateUp(ctx context.Context) {
	if s.MigrationUp == "" {
		return
	}
	conn, err := s.GetConnection(ctx)
	s.Require().NoErrorf(err, "failed to connect to PostgreSQL")
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, s.MigrationUp)
	s.Require().NoErrorf(err, "failed to run up migration")
}

func (s *PgContainerSuite) MigrateDown(ctx context.Context) {
	if s.MigrationDown == "" {
		return
	}
	conn, err := s.GetConnection(ctx)
	s.Require().NoErrorf(err, "failed to connect to PostgreSQL")
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, s.MigrationDown)
	s.Require().NoErrorf(err, "failed to run down migration")
}
