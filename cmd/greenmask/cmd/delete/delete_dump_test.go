package delete

import (
	"context"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmdInternals "github.com/greenmaskio/greenmask/internal/db/postgres/cmd"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/encryption"
)

// wrap returns root wrapped for cfg.
func wrap(t *testing.T, root storages.Storager, cfg domains.EncryptionConfig) storages.Storager {
	t.Helper()
	st, err := encryption.NewStorage(root, cfg, cmdInternals.HeartBeatFileName)
	require.NoError(t, err)
	return st
}

// putDump writes dump dumpId with the given heartbeat through root wrapped for cfg.
func putDump(t *testing.T, root storages.Storager, cfg domains.EncryptionConfig, dumpId, heartbeat string) {
	t.Helper()
	ctx := context.Background()
	st, err := wrap(t, root, cfg).SubStorage(dumpId, true)
	require.NoError(t, err)
	md := `{"startedAt":"2026-11-22T10:00:00Z","completedAt":"2026-11-22T10:01:00Z","header":{"dbName":"db"}}`
	require.NoError(t, st.PutObject(ctx, "metadata.json", strings.NewReader(md)))
	require.NoError(t, st.PutObject(ctx, "heartbeat", strings.NewReader(heartbeat)))
}

func dumpIds(dumps []*Dump) []string {
	ids := make([]string, 0, len(dumps))
	for _, d := range dumps {
		ids = append(ids, d.DumpId)
	}
	return ids
}

func TestGetSortedBackupWithStatuses_encryption(t *testing.T) {
	ctx := context.Background()
	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	withKey := domains.EncryptionConfig{Key: id.String()}

	root := inmemory.New("")
	putDump(t, root, domains.EncryptionConfig{}, "100", "done")
	putDump(t, root, withKey, "200", "done")

	started := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

	t.Run("without key classifies both", func(t *testing.T) {
		sr, err := getSortedBackupWithStatuses(ctx, wrap(t, root, domains.EncryptionConfig{}))
		require.NoError(t, err)
		assert.Equal(t, []string{"200", "100"}, dumpIds(sr.Valid))
		assert.Empty(t, sr.Failed)
		assert.Empty(t, sr.UnknownOrFailed)
		assert.Empty(t, sr.Valid[0].Database, "the metadata of the encrypted dump is unreadable without the key")
		assert.Equal(t, time.UnixMilli(200), sr.Valid[0].Date, "and the dump is dated by its id")
		assert.Equal(t, "db", sr.Valid[1].Database)
		assert.True(t, started.Equal(sr.Valid[1].Date), "a readable dump is dated by its metadata")
	})
	t.Run("with key classifies both", func(t *testing.T) {
		sr, err := getSortedBackupWithStatuses(ctx, wrap(t, root, domains.EncryptionConfig{Key: id.String()}))
		require.NoError(t, err)
		assert.Equal(t, []string{"200", "100"}, dumpIds(sr.Valid))
		assert.Equal(t, "db", sr.Valid[0].Database)
		assert.True(t, started.Equal(sr.Valid[0].Date))
	})
}

func TestGetSortedBackupWithStatuses_unreadableMetadata(t *testing.T) {
	ctx := context.Background()
	root := inmemory.New("")
	st := wrap(t, root, domains.EncryptionConfig{})
	for _, dumpId := range []string{"300", "not-a-timestamp"} {
		dump, err := st.SubStorage(dumpId, true)
		require.NoError(t, err)
		require.NoError(t, dump.PutObject(ctx, "metadata.json", strings.NewReader("not json")))
		require.NoError(t, dump.PutObject(ctx, "heartbeat", strings.NewReader("done")))
	}

	sr, err := getSortedBackupWithStatuses(ctx, st)
	require.NoError(t, err)
	require.Equal(t, []string{"300"}, dumpIds(sr.Valid), "a dump that cannot be dated is skipped, not deleted as old")
	assert.Equal(t, time.UnixMilli(300), sr.Valid[0].Date)
	assert.Empty(t, sr.Failed)
	assert.Empty(t, sr.UnknownOrFailed)
}

func TestRetainRecentNDumps_encryptedWithoutItsKey(t *testing.T) {
	ctx := context.Background()
	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	other, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	withKey := domains.EncryptionConfig{Key: id.String()}

	prevRetain, prevDryRun := retainRecent, dryRun
	t.Cleanup(func() { retainRecent, dryRun = prevRetain, prevDryRun })
	retainRecent, dryRun = 1, false

	tests := map[string]domains.EncryptionConfig{
		"no key":      {},
		"another key": {Key: other.String()},
	}
	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			root := inmemory.New("")
			putDump(t, root, withKey, "50", "in-progress")
			putDump(t, root, withKey, "100", "done")
			putDump(t, root, domains.EncryptionConfig{}, "200", "done")
			st := wrap(t, root, cfg)

			sr, err := getSortedBackupWithStatuses(ctx, st)
			require.NoError(t, err)
			require.Equal(t, []string{"200", "100"}, dumpIds(sr.Valid))
			assert.Equal(t, time.UnixMilli(100), sr.Valid[1].Date, "the encrypted dump is dated by its id")

			require.NoError(t, retainRecentNDumps(ctx, st))
			_, dirs, err := st.ListDir(ctx)
			require.NoError(t, err)
			var ids []string
			for _, d := range dirs {
				ids = append(ids, d.Dirname())
			}
			assert.ElementsMatch(t, []string{"50", "200"}, ids, "the older done dump is deleted, the in-progress one kept")
		})
	}
}
