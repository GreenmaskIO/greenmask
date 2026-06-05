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
// Both resource accessors are functional: RunWithOperationalDB scopes a minimal
// SQL-like DB to a callback, and RunWithEngineResource scopes an engine-specific
// runtime resource (as any) to a callback. The session implementation owns the
// resource lifecycle in both cases — acquiring it before fn and releasing it
// after — so callers cannot leak pooled connections or snapshots by holding a
// resource past its intended scope.
type DumpSession interface {

	// Close releases all runtime resources associated with the session.
	//
	// Implementations are responsible for:
	//   - closing pools
	//   - rolling back transactions if needed
	//   - releasing snapshots
	//   - cleaning temporary runtime resources
	Close(ctx context.Context) error

	// RunWithOperationalDB scopes a minimal common SQL-like DB to fn for the
	// duration of the call, with the session owning its lifecycle. It is used by
	// generic planning stages for introspection, metadata queries, validation,
	// and other lightweight operational SQL.
	//
	// The DB is valid only while fn runs and must not be retained afterwards.
	// As with RunWithEngineResource, a raw accessor is intentionally not offered
	// so the session impl always controls acquisition and release.
	RunWithOperationalDB(ctx context.Context, fn func(ctx context.Context, db DB) error) error

	// RunWithEngineResource borrows a DBMS-specific runtime resource for the
	// duration of fn and guarantees its release afterwards, even if fn returns
	// an error or panics.
	//
	// The resource is scoped to the fn call: it is valid only while fn runs and
	// must not be retained after fn returns. This functional form is the only way
	// to reach engine resources — handing them out for the caller to release by
	// hand is intentionally not offered, since a missed release leaks pooled
	// connections and snapshots.
	//
	// The concrete type of res is engine-specific and consumers type-assert it:
	//   - MySQL:      a pooled connection bound to the dump snapshot
	//   - PostgreSQL: e.g. a snapshot-isolated connection / pgproto3 client
	//
	// Resources may include connection pools, transactional/snapshot connections,
	// protocol-level clients, and other execution infrastructure.
	RunWithEngineResource(ctx context.Context, fn func(ctx context.Context, res any) error) error
}

type DumpSessionBuilder interface {
	Open(ctx context.Context, cfg ConnectionConfigurer) (DumpSession, error)
}
