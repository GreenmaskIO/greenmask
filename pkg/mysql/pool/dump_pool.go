package pool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	_ "github.com/go-sql-driver/mysql" // register mysql driver
	"github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
)

const (
	defaultHeartbeatInterval = 10 * time.Second
	defaultHeartbeatTimeout  = 2 * time.Second
)

type MetaTx interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// WorkerConn is a single pooled connection bound to the consistent snapshot.
//
// A worker only owns its raw protocol connection (RawConn). The shared
// snapshot-isolated SQL meta-transaction is not a per-worker concern: it lives on
// the pool and is reached through the dump session's OperationalDB (or the pool's
// GetMetaTx). Engine-specific dumpers that need raw connections borrow them via
// the pool's RunWithConn (surfaced by the session's RunWithEngineResource).
type WorkerConn interface {
	ID() int
	RawConn() *client.Conn
}

type workerConn struct {
	id   int
	Conn *client.Conn
}

func (wc *workerConn) ID() int {
	return wc.id
}

func (wc *workerConn) RawConn() *client.Conn {
	return wc.Conn
}

type metaTx struct {
	tx *sql.Tx
}

func (m *metaTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return m.tx.QueryContext(ctx, query, args...)
}

func (m *metaTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return m.tx.ExecContext(ctx, query, args...)
}

func (m *metaTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return m.tx.QueryRowContext(ctx, query, args...)
}

// ConsistentDumpTxPool implements core.DatabaseSession for the dump pipeline only.
// Restore operations must use RestorePool instead.
var _ core.DatabaseSession = (*ConsistentDumpTxPool)(nil)

// ConsistentDumpTxPool provides a pool of MySQL connections that share a consistent snapshot
// for dump operations. It implements a synchronization protocol to ensure all worker
// sessions see the exact same database state. This is achieved by:
// 1. Preparing N worker sessions with REPEATABLE READ isolation.
// 2. Acquiring a global read lock (FLUSH TABLES WITH READ LOCK) on a coordinator session.
// 3. Forcing each worker to establish its snapshot while the lock is held (dummy read).
// 4. Releasing the global lock on the coordinator session.
//
// This type is dump-only. For restore operations use RestorePool.
//
// Usage:
//
//	pool := NewConsistentTxPool(cfg, size)
//	if err := pool.Init(ctx); err != nil {
//	    pool.Close(cleanupCtx) // Close should be called even on Init failure
//	    return err
//	}
//	defer pool.Close(ctx)
//
//	err := pool.RunWithConn(ctx, func(ctx context.Context, worker WorkerConn) error {
//	    // ... perform work with worker.RawConn() ...
//	    return nil
//	}) // the connection is returned to the pool automatically
//
// Leak Notes:
//   - Connection Leak (Queue): borrowing is only possible via RunWithConn, which
//     always returns the connection to the queue when the callback exits, so a
//     worker cannot be leaked by forgetting to release it.
//   - Resource Leak (System): ALWAYS call Close(ctx). Even if Init fails partway, some connections
//     may have been opened and stored in the internal pool. Close iterates the raw pool slice
//     (not the queue) to ensure all transactions are rolled back and connections are closed.
//   - Snapshot Persistence: Transactions in the pool remain open indefinitely until Close is called.
//     Long-lived pools will keep the MySQL undo logs from being purged (History List Length).
type ConsistentDumpTxPool struct {
	cfg               *mysqlmodels.ConnConfig
	db                *sql.DB
	metaConn          *sql.Conn
	metaTx            *sql.Tx
	pool              []*workerConn
	queue             chan WorkerConn
	poolSize          int
	PreSnapshotHook   func() // Hook for testing
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
	heartbeatCancel   context.CancelFunc
	heartbeatWg       sync.WaitGroup
}

func (p *ConsistentDumpTxPool) GetMetaTx() MetaTx {
	return &metaTx{tx: p.metaTx}
}

// RunWithOperationalDB invokes fn through the shared snapshot-isolated meta
// transaction. All planning and introspection stages use this for consistent reads.
func (p *ConsistentDumpTxPool) RunWithOperationalDB(ctx context.Context, fn func(ctx context.Context, db core.DB) error) error {
	if p.metaTx == nil {
		return fmt.Errorf("dump session: pool is not initialised")
	}
	return fn(ctx, &metaTx{tx: p.metaTx})
}

// RunWithEngineResource borrows a pooled raw connection for the duration of fn and
// returns it to the pool afterwards (even on error or panic).
// The resource passed to fn is a WorkerConn; consumers type-assert it.
func (p *ConsistentDumpTxPool) RunWithEngineResource(ctx context.Context, fn func(ctx context.Context, res any) error) error {
	if p.queue == nil {
		return fmt.Errorf("dump session: pool is not initialised")
	}
	return p.RunWithConn(ctx, func(ctx context.Context, conn WorkerConn) error {
		return fn(ctx, conn)
	})
}

type Option func(*ConsistentDumpTxPool)

func WithHeartbeat(interval time.Duration) Option {
	return func(p *ConsistentDumpTxPool) {
		if interval <= 0 {
			interval = defaultHeartbeatInterval
		}
		p.heartbeatInterval = interval
	}
}

func WithHeartbeatTimeout(timeout time.Duration) Option {
	return func(p *ConsistentDumpTxPool) {
		if timeout <= 0 {
			timeout = defaultHeartbeatTimeout
		}
		p.heartbeatTimeout = timeout
	}
}

func NewConsistentTxPool(cfg *mysqlmodels.ConnConfig, poolSize int, opts ...Option) *ConsistentDumpTxPool {
	p := &ConsistentDumpTxPool{
		cfg:              cfg,
		poolSize:         poolSize,
		heartbeatTimeout: defaultHeartbeatTimeout, // Default heartbeat timeout
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// connectRaw establishes a raw go-mysql connection.
func (p *ConsistentDumpTxPool) connectRaw(ctx context.Context) (*client.Conn, error) {
	var opts []client.Option
	if p.cfg.TLSConfig != nil {
		opts = append(opts, func(c *client.Conn) error {
			c.SetTLSConfig(p.cfg.TLSConfig)
			return nil
		})
	}

	var conn *client.Conn
	var err error

	if p.cfg.Socket != "" {
		dialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", p.cfg.Socket)
		}
		conn, err = client.ConnectWithDialer(
			ctx, "unix", p.cfg.Socket,
			p.cfg.User, p.cfg.Password, p.cfg.Database,
			dialer, opts...,
		)
	} else {
		conn, err = client.ConnectWithContext(
			ctx, p.cfg.Address(), p.cfg.User, p.cfg.Password, p.cfg.Database, p.cfg.Timeout, opts...,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("connect to mysql (raw): %w", err)
	}
	return conn, nil
}

// connectSql establishes a database/sql connection.
func (p *ConsistentDumpTxPool) connectSql(ctx context.Context) (*sql.Conn, error) {
	if p.db == nil {
		uri, err := p.cfg.URI()
		if err != nil {
			return nil, fmt.Errorf("get connection URI: %w", err)
		}
		db, err := sql.Open("mysql", uri)
		if err != nil {
			return nil, fmt.Errorf("open mysql db: %w", err)
		}
		p.db = db
	}

	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("get sql connection: %w", err)
	}
	log.Ctx(ctx).Debug().Msg("opened sql mysql connection")
	return conn, nil
}

func (p *ConsistentDumpTxPool) prepareWorkerConns(ctx context.Context) error {
	log.Ctx(ctx).Debug().Msgf("phase 0: preparing %d worker sessions", p.poolSize)
	p.pool = make([]*workerConn, p.poolSize)
	for i := 0; i < p.poolSize; i++ {
		// Prepare raw connection
		rawConn, err := p.connectRaw(ctx)
		if err != nil {
			return fmt.Errorf("create worker raw connection %d: %w", i, err)
		}
		log.Ctx(ctx).Debug().
			Int("connID", i).
			Msg("opened raw mysql connection")

		if _, err := rawConn.Execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			if err := rawConn.Close(); err != nil {
				log.Ctx(ctx).Warn().Err(err).Int("connID", i).
					Msg("failed to close raw connection after isolation level setup failure")
			}
			return fmt.Errorf("set isolation level on worker raw %d: %w", i, err)
		}

		p.pool[i] = &workerConn{
			id:   i,
			Conn: rawConn,
		}
	}
	return nil
}

func (p *ConsistentDumpTxPool) synchronizeSnapshots(ctx context.Context) error {
	// Phase 1: Acquire Synchronization Lock (Coordinator Session)
	log.Ctx(ctx).Debug().Msg("phase 1: acquiring synchronization lock")
	coord, err := p.connectRaw(ctx)
	if err != nil {
		return fmt.Errorf("create coordinator connection: %w", err)
	}
	defer func() {
		if err := coord.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("close coordinator connection")
		} else {
			log.Ctx(ctx).Debug().Msg("closed coordinator raw mysql connection")
		}
	}()

	log.Ctx(ctx).Debug().Msg("phase 1: acquiring global read lock (FTWRL)")
	if _, err := coord.Execute("FLUSH TABLES WITH READ LOCK"); err != nil {
		return fmt.Errorf("acquire global lock: %w", err)
	}
	log.Ctx(ctx).Debug().Msg("phase 1: FTWRL acquired")
	if p.PreSnapshotHook != nil {
		p.PreSnapshotHook()
	}
	defer func() {
		log.Ctx(ctx).Debug().Msg("phase 3: releasing global read lock")
		if _, err := coord.Execute("UNLOCK TABLES"); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to release global lock")
		}
	}()

	// Phase 2: Establish Consistent Snapshots (All Workers)
	log.Ctx(ctx).Debug().Msg("phase 2: establishing consistent snapshots")

	// 2.1: Start Shared Meta SQL Transaction
	metaConn, err := p.connectSql(ctx)
	if err != nil {
		return fmt.Errorf("get shared meta connection: %w", err)
	}
	p.metaConn = metaConn

	_, err = p.metaConn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	if err != nil {
		return fmt.Errorf("set isolation level on meta connection: %w", err)
	}

	metaTx, err := p.metaConn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("begin meta sql tx: %w", err)
	}
	p.metaTx = metaTx

	// Force snapshot for meta tx
	if _, err := p.metaTx.ExecContext(ctx, "SELECT 1 FROM information_schema.tables LIMIT 1"); err != nil {
		return fmt.Errorf("force meta sql snapshot: %w", err)
	}

	g, _ := errgroup.WithContext(ctx)
	for i := 0; i < p.poolSize; i++ {
		worker := p.pool[i]
		g.Go(func() error {
			log.Ctx(ctx).Debug().
				Int("connID", worker.id).
				Msg("starting tx in pool connection")
			// Start Raw Transaction
			if err := worker.Conn.BeginTx(true, "REPEATABLE READ"); err != nil {
				return fmt.Errorf("begin raw tx: %w", err)
			}

			// Force snapshots while locked
			if _, err := worker.Conn.Execute("SELECT 1 FROM information_schema.tables LIMIT 1"); err != nil {
				return fmt.Errorf("force raw snapshot: %w", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("establish consistent snapshots: %w", err)
	}

	log.Ctx(ctx).Debug().Msg("phase 2: consistent snapshots established")

	return nil
}

func (p *ConsistentDumpTxPool) Init(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			// Use a background context for cleanup to ensure it runs even if the original context is cancelled
			if closeErr := p.Close(context.Background()); closeErr != nil {
				log.Ctx(ctx).Warn().Err(closeErr).Msg("failed to close pool after initialization error")
			}
		}
	}()

	if err = p.prepareWorkerConns(ctx); err != nil {
		return err
	}

	if err = p.synchronizeSnapshots(ctx); err != nil {
		return err
	}

	p.queue = make(chan WorkerConn, p.poolSize)
	for _, conn := range p.pool {
		p.queue <- conn
	}

	if p.heartbeatInterval > 0 {
		heartbeatCtx, cancel := context.WithCancel(ctx)
		p.heartbeatCancel = cancel
		p.heartbeatWg.Add(1)
		go p.heartbeatWorker(heartbeatCtx)
	}

	return nil
}

// RunWithConn borrows a worker connection from the pool, invokes fn with it, and
// always returns the connection to the pool once fn completes — including when fn
// returns an error or panics.
//
// It is the only way to obtain a pooled connection. Hand-paired acquire/release
// is intentionally not exposed: a missed release permanently shrinks the pool and
// eventually blocks every caller, so the borrow is scoped to fn instead.
//
// The connection is returned via a deferred send that runs even on panic. The
// queue always has room for a connection that was just taken from it, so the
// return never blocks and needs no context.
func (p *ConsistentDumpTxPool) RunWithConn(
	ctx context.Context,
	fn func(ctx context.Context, conn WorkerConn) error,
) error {
	var conn WorkerConn
	select {
	case conn = <-p.queue:
		log.Ctx(ctx).Debug().Int("connID", conn.ID()).Msg("acquired connection from pool")
	case <-ctx.Done():
		return fmt.Errorf("acquire worker connection: %w", ctx.Err())
	}
	defer func() {
		p.queue <- conn
		log.Ctx(ctx).Debug().Int("connID", conn.ID()).Msg("returned connection to pool")
	}()
	return fn(ctx, conn)
}

func (p *ConsistentDumpTxPool) Close(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		logger := log.Ctx(ctx)
		ctx := logger.WithContext(context.Background())
		done <- p.close(ctx)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// closeWorkerConn rolls back and closes a single raw worker connection.
// Both operations are attempted regardless; all errors are joined and returned.
func (p *ConsistentDumpTxPool) closeWorkerConn(ctx context.Context, conn *workerConn) error {
	if conn == nil || conn.Conn == nil {
		return nil
	}
	var errs []error
	if err := conn.Conn.Rollback(); err != nil {
		errs = append(errs, fmt.Errorf("rollback raw tx for conn %d: %w", conn.id, err))
	}
	if err := conn.Conn.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close raw conn %d: %w", conn.id, err))
	}
	log.Ctx(ctx).Debug().Int("connID", conn.id).Msg("closed raw connection in pool")
	return errors.Join(errs...)
}

func (p *ConsistentDumpTxPool) close(ctx context.Context) error {
	if p.heartbeatCancel != nil {
		p.heartbeatCancel()
		p.heartbeatWg.Wait()
	}

	var errs []error

	if p.metaTx != nil {
		if err := p.metaTx.Rollback(); err != nil {
			errs = append(errs, fmt.Errorf("rollback meta sql tx: %w", err))
		}
	}

	if p.metaConn != nil {
		if err := p.metaConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close meta sql conn: %w", err))
		} else {
			log.Ctx(ctx).Debug().Msg("closed meta sql mysql connection")
		}
	}

	if p.pool != nil {
		connErrs := make([]error, len(p.pool))
		g, _ := errgroup.WithContext(ctx)
		for i, conn := range p.pool {
			g.Go(func() error {
				connErrs[i] = p.closeWorkerConn(ctx, conn)
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			// goroutines never return errors; all errors stored in connErrs
			// but anyway let's handle it just in case
			errs = append(errs, fmt.Errorf("close pool: %w", err))
		}
		errs = append(errs, connErrs...)
	}

	if p.db != nil {
		if err := p.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close db: %w", err))
		}
	}

	return errors.Join(errs...)
}

func (p *ConsistentDumpTxPool) heartbeatWorker(ctx context.Context) {
	defer p.heartbeatWg.Done()
	ticker := time.NewTicker(p.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.runHeartbeat(ctx)
		}
	}
}

func (p *ConsistentDumpTxPool) runHeartbeat(ctx context.Context) {
	var conns []WorkerConn
	defer func() {
		for _, conn := range conns {
			p.queue <- conn
		}
	}()

	for i := 0; i < p.poolSize; i++ {
		stop := false
		select {
		case conn := <-p.queue:
			conns = append(conns, conn)
		default:
			stop = true
		}
		if stop {
			break
		}
	}

	if len(conns) == 0 {
		return
	}

	log.Ctx(ctx).Debug().Int("count", len(conns)).Msg("executing heartbeat on idle sessions")
	g, _ := errgroup.WithContext(ctx)

	// Heartbeat on shared meta tx
	if p.metaTx != nil {
		g.Go(func() error {
			if _, err := p.metaTx.ExecContext(ctx, "SELECT 1"); err != nil {
				log.Ctx(ctx).Warn().Err(err).Msg("failed to execute meta sql heartbeat query")
				return err
			}
			return nil
		})
	}

	for _, conn := range conns {
		g.Go(func() error {
			// Heartbeat on raw connection
			if _, err := conn.RawConn().Execute("SELECT 1"); err != nil {
				log.Ctx(ctx).Warn().Err(err).Msg("failed to execute raw heartbeat query")
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("heartbeat group error")
	}
}
