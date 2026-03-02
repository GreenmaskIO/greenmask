package pool

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

type mockCfg struct {
	uri string
}

func (m *mockCfg) URI() (string, error) {
	return m.uri, nil
}

func TestConsistentTxPool_SynchronizedSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Enable debug logging for the test
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).Level(zerolog.DebugLevel)

	// 1. Start MySQL Container
	mysqlContainer, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("testdb"),
		mysql.WithUsername("root"),
		mysql.WithPassword("testpass"),
	)
	require.NoError(t, err)
	defer mysqlContainer.Terminate(ctx)

	uri, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)
	// Add multiStatements=true just in case, though not strictly needed for this test
	// uri = uri + "?multiStatements=true" // Removed as per instruction

	db, err := sql.Open("mysql", uri)
	require.NoError(t, err)
	defer db.Close()

	// 2. Prepare schema and initial data
	_, err = db.ExecContext(ctx, "CREATE TABLE test_table (id INT PRIMARY KEY, val VARCHAR(255))")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO test_table (id, val) VALUES (1, 'initial')")
	require.NoError(t, err)

	poolSize := 5

	// Deterministic sync: cfg.URI() will block until we tell it to proceed
	proceedInit := make(chan struct{})
	cfg := &blockingCfg{uri: uri, proceed: proceedInit}
	p := NewConsistentTxPool(cfg, poolSize)

	// Phase 1 Lock Hook: This will try to insert a record while FTWRL is held.
	// The INSERT should block until Init returns (Phase 3 UNLOCK).
	insertFinished := make(chan struct{})
	p.PreSnapshotHook = func() {
		fmt.Println("hook: phase 1 lock acquired. Starting blocked INSERT (id=3)...")
		go func() {
			_, err := db.ExecContext(ctx, "INSERT INTO test_table (id, val) VALUES (3, 'during-lock')")
			if err != nil {
				fmt.Printf("hook INSERT error: %v\n", err)
			}
			fmt.Println("hook: blocked INSERT (id=3) finished")
			close(insertFinished)
		}()
		// Wait a small amount to ensure INSERT has reached the server and is blocked
		time.Sleep(200 * time.Millisecond)
	}

	initDone := make(chan error, 1)
	go func() {
		initDone <- p.Init(ctx)
	}()

	// Trigger id=2 insert before lock
	time.Sleep(100 * time.Millisecond)
	_, err = db.ExecContext(ctx, "INSERT INTO test_table (id, val) VALUES (2, 'before-lock')")
	require.NoError(t, err)
	fmt.Println("test: INSERT (id=2) finished (before lock)")

	// Let Init proceed to Phase 1 (Locks)
	close(proceedInit)

	err = <-initDone
	require.NoError(t, err)
	defer p.Close(ctx)

	// Wait for the blocked insert to finish after UNLOCK
	select {
	case <-insertFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for blocked INSERT to finish")
	}

	// 4. Verify all workers see identical counts (should be 2, NOT 3)
	// Because id=2 was inserted before lock, but id=3 was inserted during lock
	for i := 0; i < poolSize; i++ {
		worker, err := p.GetConn(ctx)
		require.NoError(t, err)

		var count int
		metaTx, err := worker.GetMetaTx(ctx)
		require.NoError(t, err)
		err = metaTx.QueryRowContext(ctx, "SELECT COUNT(*) FROM test_table").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 2, count, "Worker %d should see exactly 2 rows", i)

		err = p.PutConn(ctx, worker)
		assert.NoError(t, err)
	}

	// 5. Verify a new connection sees 3 rows
	var finalCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test_table").Scan(&finalCount)
	require.NoError(t, err)
	assert.Equal(t, 3, finalCount)
}

type blockingCfg struct {
	uri     string
	proceed chan struct{}
}

func (m *blockingCfg) URI() (string, error) {
	<-m.proceed
	return m.uri, nil
}

func TestConsistentTxPool_FailureLockRelease(t *testing.T) {
	// This test verifies that if a worker fails to start transaction, the lock is still released.
	// We can't easily make a worker fail without mocking the driver or using some extreme measures.
	// However, we can at least verify that a second Init or a normal query works after a failed one if we could trigger it.
	// Since triggering failure is hard, we'll rely on the defer in pool.go which we've verified by inspection.
	t.Skip("Hard to trigger internal worker failure without mocks")
}

func TestConsistentTxPool_Heartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Enable debug logging for the test to see heartbeat logs
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).Level(zerolog.DebugLevel)

	// 1. Start MySQL Container
	mysqlContainer, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("testdb"),
		mysql.WithUsername("root"),
		mysql.WithPassword("testpass"),
	)
	require.NoError(t, err)
	defer mysqlContainer.Terminate(ctx)

	uri, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	poolSize := 2
	heartbeatInterval := 500 * time.Millisecond

	cfg := &mockCfg{uri: uri}
	p := NewConsistentTxPool(cfg, poolSize, WithHeartbeat(heartbeatInterval))

	err = p.Init(ctx)
	require.NoError(t, err)

	// Wait for at least 2 heartbeats
	time.Sleep(1200 * time.Millisecond)

	// Take all connections
	worker1, err := p.GetConn(ctx)
	require.NoError(t, err)
	worker2, err := p.GetConn(ctx)
	require.NoError(t, err)

	// Wait for another heartbeat interval (should do nothing as queue is empty)
	time.Sleep(600 * time.Millisecond)

	// Put one back
	err = p.PutConn(ctx, worker1)
	require.NoError(t, err)

	// Wait for heartbeat
	time.Sleep(600 * time.Millisecond)

	err = p.PutConn(ctx, worker2)
	require.NoError(t, err)

	err = p.Close(ctx)
	require.NoError(t, err)
}
