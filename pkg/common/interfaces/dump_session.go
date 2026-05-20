package interfaces

import "context"

// DumpSession represents a database-specific runtime session
// shared across the entire dump pipeline execution.
//
// A session is created once at the beginning of the dump pipeline
// and remains alive until the dump process finishes.
//
// The session encapsulates all runtime database resources required
// for consistent dump execution, including:
//   - operational database connections
//   - transaction/snapshot state
//   - worker connection pools
//   - engine-specific runtime handles
//   - protocol-level dump connections
//
// The same session instance is shared between planning and execution stages,
// allowing all stages to operate against a consistent database state.
//
// Examples:
//
// PostgreSQL:
//   - pgx pools
//   - exported snapshots
//   - transactional connections
//   - pgproto3 clients
//
// MySQL:
//   - synchronized transactional connections
//   - dump protocol pools
//   - operational SQL connections
//
// OperationalDB provides a minimal common SQL-like interface
// used by generic stages such as introspection, validation,
// and metadata collection.
//
// Runtime exposes engine-specific runtime capabilities required
// by DBMS-specific dumpers and factories.
//
// Runtime intentionally returns any because different DBMS engines
// require fundamentally different runtime implementations and
// connection management models.
//
// Concrete runtime type assertions are expected to happen only
// inside DBMS-specific implementations such as dump factories
// and dumpers.
type DumpSession interface {

	// Close releases all runtime resources associated with the session.
	//
	// Implementations are responsible for:
	//   - closing pools
	//   - rolling back transactions if needed
	//   - releasing snapshots
	//   - cleaning temporary runtime resources
	Close(ctx context.Context) error

	// OperationalDB returns a minimal common SQL-like database interface
	// used by generic planning stages.
	//
	// This connection is intended for:
	//   - introspection
	//   - metadata queries
	//   - validation
	//   - lightweight operational SQL
	OperationalDB(ctx context.Context) (DB, error)

	// EngineResources returns DBMS-specific runtime resources
	// associated with the current dump session.
	//
	// Examples:
	//   - *PostgresResources
	//   - *MySQLResources
	//   - *OracleResources
	//
	// Resources may include:
	//   - connection pools
	//   - transactional connections
	//   - snapshot/session managers
	//   - protocol-level clients
	//   - execution-specific infrastructure
	//
	// EngineResources is primarily consumed by DBMS-specific
	// factories, dumpers, and low-level execution components.
	EngineResources(ctx context.Context) (any, error)
}

type DumpSessionBuilder interface {
	Open(ctx context.Context, cfg ConnectionConfigurer) (DumpSession, error)
}
