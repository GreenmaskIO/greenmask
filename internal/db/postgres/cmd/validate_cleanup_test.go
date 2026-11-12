package cmd

import (
	"bytes"
	"context"
	"testing"

	"github.com/greenmaskio/storages/inmemory"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
)

func TestValidate_Run_diff_removes_temporary_directory(t *testing.T) {
	ctx := context.Background()
	cfg := &domains.Config{Validate: domains.Validate{Diff: true}}
	// Nothing listens on port 1, so Run fails at connect and only the cleanup is exercised.
	cfg.Dump.PgDumpOptions.Host = "127.0.0.1"
	cfg.Dump.PgDumpOptions.Port = 1

	st := inmemory.New("")
	v, err := NewValidate(cfg, utils.NewTransformerRegistry(), st, nil)
	require.NoError(t, err)
	require.NoError(t, v.st.PutObject(ctx, "1.dat.gz", bytes.NewBufferString("data")))

	_, err = v.Run(ctx)
	require.Error(t, err)

	_, dirs, err := st.ListDir(ctx)
	require.NoError(t, err)
	assert.Empty(t, dirs)
}

func TestValidate_Run_diff_cleanup_of_empty_temporary_directory_does_not_warn(t *testing.T) {
	prev := log.Logger
	buf := &bytes.Buffer{}
	log.Logger = zerolog.New(buf)
	t.Cleanup(func() { log.Logger = prev })

	cfg := &domains.Config{Validate: domains.Validate{Diff: true}}
	cfg.Dump.PgDumpOptions.Host = "127.0.0.1"
	cfg.Dump.PgDumpOptions.Port = 1

	v, err := NewValidate(cfg, utils.NewTransformerRegistry(), inmemory.New(""), nil)
	require.NoError(t, err)
	_, err = v.Run(context.Background())
	require.Error(t, err)

	assert.NotContains(t, buf.String(), "error deleting temporary directory")
}
