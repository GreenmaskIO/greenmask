package pool

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	sqldriver "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
)

// connConfigFromURI parses a go-sql-driver DSN into a *mysqlmodels.ConnConfig.
func connConfigFromURI(uri string) (*mysqlmodels.ConnConfig, error) {
	cfg, err := sqldriver.ParseDSN(uri)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}
	parts := strings.SplitN(cfg.Addr, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("unexpected Addr format %q", cfg.Addr)
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("parse port %q: %w", parts[1], err)
	}
	return &mysqlmodels.ConnConfig{
		Host:     parts[0],
		Port:     port,
		User:     cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
	}, nil
}

func TestConsistentTxPool_SynchronizedSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).Level(zerolog.DebugLevel)

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

	db, err := sql.Open("mysql", uri)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, "CREATE TABLE test_table (id INT PRIMARY KEY, val VARCHAR(255))")
	require.NoError(t, err)
	// id=1 and id=2 are committed before pool init — all workers must see them.
	_, err = db.ExecContext(ctx, "INSERT INTO test_table (id, val) VALUES (1, 'initial'), (2, 'before-pool')")
	require.NoError(t, err)

	poolSize := 5

	connCfg, err := connConfigFromURI(uri)
	require.NoError(t, err)

	p := NewConsistentTxPool(connCfg, poolSize)

	// PreSnapshotHook fires after FTWRL is acquired and before consistent snapshots are taken.
	// We attempt id=3 here; it blocks until FTWRL is released, so workers must NOT see it.
	insertFinished := make(chan struct{})
	p.PreSnapshotHook = func() {
		fmt.Println("hook: FTWRL acquired. Starting blocked INSERT (id=3)...")
		go func() {
			_, err := db.ExecContext(ctx, "INSERT INTO test_table (id, val) VALUES (3, 'during-lock')")
			if err != nil {
				fmt.Printf("hook INSERT error: %v\n", err)
			}
			fmt.Println("hook: blocked INSERT (id=3) finished")
			close(insertFinished)
		}()
		time.Sleep(200 * time.Millisecond)
	}

	require.NoError(t, p.Init(ctx))
	defer p.Close(ctx)

	select {
	case <-insertFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for blocked INSERT to finish")
	}

	// All workers must see exactly 2 rows (id=1 and id=2).
	// id=3 was committed after FTWRL — it must not appear in any worker snapshot.
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

	var finalCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test_table").Scan(&finalCount)
	require.NoError(t, err)
	assert.Equal(t, 3, finalCount)
}

func TestConsistentTxPool_FailureLockRelease(t *testing.T) {
	t.Skip("Hard to trigger internal worker failure without mocks")
}

func TestConsistentTxPool_Heartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).Level(zerolog.DebugLevel)

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

	connCfg, err := connConfigFromURI(uri)
	require.NoError(t, err)

	poolSize := 2
	heartbeatInterval := 500 * time.Millisecond

	p := NewConsistentTxPool(connCfg, poolSize, WithHeartbeat(heartbeatInterval))

	err = p.Init(ctx)
	require.NoError(t, err)

	time.Sleep(1200 * time.Millisecond)

	worker1, err := p.GetConn(ctx)
	require.NoError(t, err)
	worker2, err := p.GetConn(ctx)
	require.NoError(t, err)

	time.Sleep(600 * time.Millisecond)

	err = p.PutConn(ctx, worker1)
	require.NoError(t, err)

	time.Sleep(600 * time.Millisecond)

	err = p.PutConn(ctx, worker2)
	require.NoError(t, err)

	err = p.Close(ctx)
	require.NoError(t, err)
}
