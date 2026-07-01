package core

import (
	"context"
	"fmt"
)

// ErrEngineResourceNotSupported is returned by DatabaseSession.RunWithEngineResource
// implementations that do not support engine-specific resources (e.g. restore sessions).
var ErrEngineResourceNotSupported = fmt.Errorf("engine resource not supported by this session")

// DatabaseSession is the unified live-connection interface for both dump and
// restore pipelines.
//
// Dump implementations use RunWithEngineResource for snapshot isolation and
// parallel connection pools. Restore implementations provide a no-op or
// unsupported stub — a single operational connection suffices on the write side.
type DatabaseSession interface {
	// Close releases all resources associated with the session.
	Close(ctx context.Context) error

	// RunWithOperationalDB scopes a minimal SQL-like DB to fn for the duration
	// of the call. The session owns the DB lifecycle; fn must not retain the DB
	// after it returns.
	RunWithOperationalDB(ctx context.Context, fn func(ctx context.Context, db DB) error) error

	// RunWithEngineResource borrows a DBMS-specific runtime resource for the
	// duration of fn and guarantees its release afterwards.
	//
	// The resource is valid only while fn runs and must not be retained after
	// fn returns. The concrete type is engine-specific and consumers type-assert:
	//   - MySQL dump:      a pooled connection bound to the dump snapshot
	//   - PostgreSQL dump: a snapshot-isolated connection / pgproto3 client
	//
	// Restore implementations that do not use engine resources may return
	// ErrEngineResourceNotSupported.
	RunWithEngineResource(ctx context.Context, fn func(ctx context.Context, res any) error) error
}

// DatabaseSessionBuilder opens a DBMS-specific DatabaseSession from a
// ConnectionConfigurer.
type DatabaseSessionBuilder interface {
	Open(ctx context.Context, cfg ConnectionConfigurer) (DatabaseSession, error)
}
