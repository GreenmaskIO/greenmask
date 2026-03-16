package pool

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
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

type WorkerConn interface {
	ID() int
	RawConn() *client.Conn
	GetMetaTx(ctx context.Context) (MetaTx, error)
	PutMetaTx(ctx context.Context, tx MetaTx) error
}

type workerConn struct {
	id         int
	Conn       *client.Conn
	metaTx     *sql.Tx
	connConfig *mysqlmodels.ConnConfig
}

func (wc *workerConn) ID() int {
	return wc.id
}

func (wc *workerConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return wc.metaTx.QueryContext(ctx, query, args...)
}

func (wc *workerConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return wc.metaTx.ExecContext(ctx, query, args...)
}

func (wc *workerConn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return wc.metaTx.QueryRowContext(ctx, query, args...)
}

func (wc *workerConn) RawConn() *client.Conn {
	return wc.Conn
}

func (wc *workerConn) GetMetaTx(ctx context.Context) (MetaTx, error) {
	return &metaTx{tx: wc.metaTx}, nil
}

func (wc *workerConn) PutMetaTx(ctx context.Context, tx MetaTx) error {
	return nil
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

// ConsistentTxPool provides a pool of MySQL connections that share a consistent snapshot.
// It implements a synchronization protocol to ensure all worker sessions see the exact same
// database state. This is achieved by:
// 1. Preparing N worker sessions with REPEATABLE READ isolation.
// 2. Acquiring a global read lock (FLUSH TABLES WITH READ LOCK) on a coordinator session.
// 3. Forcing each worker to establish its snapshot while the lock is held (dummy read).
// 4. Releasing the global lock on the coordinator session.
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
//	worker, _ := pool.GetConn(ctx)
//	// ... perform work with worker.Tx ...
//	pool.PutConn(ctx, worker) // CRITICAL: Always return connection to the queue
//
// Leak Notes:
//   - Connection Leak (Queue): Every GetConn call must be paired with exactly one PutConn call.
//     Failure to return a worker to the queue will eventually exhaust the pool, causing callers to block.
//   - Resource Leak (System): ALWAYS call Close(ctx). Even if Init fails partway, some connections
//     may have been opened and stored in the internal pool. Close iterates the raw pool slice
//     (not the queue) to ensure all transactions are rolled back and connections are closed.
//   - Snapshot Persistence: Transactions in the pool remain open indefinitely until Close is called.
//     Long-lived pools will keep the MySQL undo logs from being purged (History List Length).
type ConsistentTxPool struct {
	cfg               interfaces.ConnectionConfigurator
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

func (p *ConsistentTxPool) GetMetaTx() MetaTx {
	return &metaTx{tx: p.metaTx}
}

type Option func(*ConsistentTxPool)

func WithHeartbeat(interval time.Duration) Option {
	return func(p *ConsistentTxPool) {
		if interval <= 0 {
			interval = defaultHeartbeatInterval
		}
		p.heartbeatInterval = interval
	}
}

func WithHeartbeatTimeout(timeout time.Duration) Option {
	return func(p *ConsistentTxPool) {
		if timeout <= 0 {
			timeout = defaultHeartbeatTimeout
		}
		p.heartbeatTimeout = timeout
	}
}

func NewConsistentTxPool(cfg interfaces.ConnectionConfigurator, poolSize int, opts ...Option) *ConsistentTxPool {
	p := &ConsistentTxPool{
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
func (p *ConsistentTxPool) connectRaw(ctx context.Context) (*client.Conn, error) {
	var address, user, password, database string
	var timeout time.Duration

	if cfg, ok := p.cfg.(*mysqlmodels.ConnConfig); ok {
		address = cfg.Address()
		user = cfg.User
		password = cfg.Password
		database = cfg.Database
		timeout = cfg.Timeout
	} else {
		uri, err := p.cfg.URI()
		if err != nil {
			return nil, fmt.Errorf("get connection URI: %w", err)
		}
		config, err := mysql.ParseDSN(uri)
		if err != nil {
			return nil, fmt.Errorf("parse connection URI: %w", err)
		}
		address = config.Addr
		user = config.User
		password = config.Passwd
		database = config.DBName
		timeout = config.Timeout
	}

	conn, err := client.ConnectWithContext(
		ctx, address, user, password, database, timeout,
	)
	if err != nil {
		return nil, fmt.Errorf("connect to mysql (raw): %w", err)
	}
	return conn, nil
}

// connectSql establishes a database/sql connection.
func (p *ConsistentTxPool) connectSql(ctx context.Context) (*sql.Conn, error) {
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

func (p *ConsistentTxPool) prepareWorkerConns(ctx context.Context) error {
	log.Ctx(ctx).Debug().Msgf("phase 0: preparing %d worker sessions", p.poolSize)
	p.pool = make([]*workerConn, p.poolSize)
	cfg, ok := p.cfg.(*mysqlmodels.ConnConfig)
	if !ok {
		return fmt.Errorf("invalid connection configurator type: expected *mysqlmodels.ConnConfig, got %T", p.cfg)
	}

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
			id:         i,
			Conn:       rawConn,
			connConfig: cfg,
		}
	}
	return nil
}

func (p *ConsistentTxPool) synchronizeSnapshots(ctx context.Context) error {
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
		worker.metaTx = p.metaTx
		g.Go(func() error {
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

	return nil
}

func (p *ConsistentTxPool) Init(ctx context.Context) (err error) {
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

func (p *ConsistentTxPool) GetConn(ctx context.Context) (WorkerConn, error) {
	select {
	case conn := <-p.queue:
		log.Ctx(ctx).Debug().Int("connID", conn.ID()).Msg("acquired connection from pool")
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *ConsistentTxPool) PutConn(ctx context.Context, conn WorkerConn) error {
	select {
	case p.queue <- conn:
		log.Ctx(ctx).Debug().Int("connID", conn.ID()).Msg("returned connection to pool")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *ConsistentTxPool) Close(ctx context.Context) error {
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

func (p *ConsistentTxPool) close(ctx context.Context) error {
	if p.heartbeatCancel != nil {
		p.heartbeatCancel()
		p.heartbeatWg.Wait()
	}

	var errs []error
	var mu sync.Mutex
	var wg sync.WaitGroup

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
		for i := range p.pool {
			wg.Add(1)
			go func(conn *workerConn) {
				defer wg.Done()
				if conn == nil {
					return
				}
				if conn.Conn != nil {
					if err := conn.Conn.Rollback(); err != nil {
						mu.Lock()
						errs = append(errs, fmt.Errorf("rollback conn raw tx: %w", err))
						mu.Unlock()
					}
					if err := conn.Conn.Close(); err != nil {
						mu.Lock()
						errs = append(errs, fmt.Errorf("close conn raw conn: %w", err))
						mu.Unlock()
					} else {
						log.Ctx(ctx).Debug().Int("connID", conn.id).Msg("closed raw connection in pool")
					}
				}
			}(p.pool[i])
		}
	}
	wg.Wait()

	if p.db != nil {
		if err := p.db.Close(); err != nil {
			mu.Lock()
			errs = append(errs, fmt.Errorf("close db: %w", err))
			mu.Unlock()
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing pool: %v", errs)
	}
	return nil
}

func (p *ConsistentTxPool) heartbeatWorker(ctx context.Context) {
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

func (p *ConsistentTxPool) runHeartbeat(ctx context.Context) {
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
		conn := conn
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
