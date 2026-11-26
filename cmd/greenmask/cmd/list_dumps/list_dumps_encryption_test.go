package list_dumps

import (
	"context"
	"strings"
	"testing"

	"filippo.io/age"
	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmdInternals "github.com/greenmaskio/greenmask/internal/db/postgres/cmd"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/encryption"
	"github.com/greenmaskio/greenmask/internal/utils/dumpstatus"
)

// wrap returns root wrapped for cfg.
func wrap(t *testing.T, root storages.Storager, cfg domains.EncryptionConfig) storages.Storager {
	t.Helper()
	st, err := encryption.NewStorage(root, cfg, cmdInternals.HeartBeatFileName)
	require.NoError(t, err)
	return st
}

// putDoneDump writes a finished dump dumpId through root wrapped for cfg.
func putDoneDump(t *testing.T, root storages.Storager, cfg domains.EncryptionConfig, dumpId string) {
	t.Helper()
	ctx := context.Background()
	st, err := wrap(t, root, cfg).SubStorage(dumpId, true)
	require.NoError(t, err)
	md := `{"startedAt":"2026-11-22T10:00:00Z","completedAt":"2026-11-22T10:01:00Z","header":{"dbName":"db"}}`
	require.NoError(t, st.PutObject(ctx, "metadata.json", strings.NewReader(md)))
	require.NoError(t, st.PutObject(ctx, "heartbeat", strings.NewReader("done")))
}

func TestRenderListItem_encryption(t *testing.T) {
	ctx := context.Background()
	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	root := inmemory.New("")
	putDoneDump(t, root, domains.EncryptionConfig{}, "100")
	putDoneDump(t, root, domains.EncryptionConfig{Key: id.String()}, "200")

	tests := []struct {
		name   string
		enc    domains.EncryptionConfig
		wantDb map[string]string
	}{
		{name: "without key", enc: domains.EncryptionConfig{}, wantDb: map[string]string{"100": "db", "200": ""}},
		{name: "with key", enc: domains.EncryptionConfig{Key: id.String()}, wantDb: map[string]string{"100": "db", "200": "db"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, dirs, err := wrap(t, root, tt.enc).ListDir(ctx)
			require.NoError(t, err)
			var rows [][]string
			for _, d := range dirs {
				require.NoError(t, renderListItem(ctx, d, &rows))
			}
			status, encrypted, dbName := map[string]string{}, map[string]string{}, map[string]string{}
			for _, r := range rows {
				dbName[r[0]] = r[2]
				status[r[0]] = r[7]
				encrypted[r[0]] = r[8]
			}
			assert.Equal(t, map[string]string{"100": dumpstatus.DoneStatusName, "200": dumpstatus.DoneStatusName}, status)
			assert.Equal(t, map[string]string{"100": "false", "200": "true"}, encrypted)
			assert.Equal(t, tt.wantDb, dbName, "the database of an encrypted dump is shown only with the key")
		})
	}
}

func TestRenderListItem_plainDumpWithBrokenMetadata(t *testing.T) {
	ctx := context.Background()
	root := inmemory.New("")
	st, err := wrap(t, root, domains.EncryptionConfig{}).SubStorage("100", true)
	require.NoError(t, err)
	require.NoError(t, st.PutObject(ctx, "metadata.json", strings.NewReader("not json")))
	require.NoError(t, st.PutObject(ctx, "heartbeat", strings.NewReader("done")))

	var rows [][]string
	require.Error(t, renderListItem(ctx, st, &rows), "a plain dump with unreadable metadata is an error, as before")
	assert.Empty(t, rows)
}
