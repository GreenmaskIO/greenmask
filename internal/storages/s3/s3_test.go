package s3

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func getCfg() *Config {
	cfg := NewConfig()
	cfg.Endpoint = "http://localhost:9000"
	cfg.Bucket = "testbucket"
	cfg.Region = "us-east-1"
	cfg.AccessKeyId = "Q3AM3UQ867SPQQA43P2F"
	cfg.SecretAccessKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	return cfg
}

func TestNewStorage(t *testing.T) {
	cfg := getCfg()
	_, err := NewStorage(context.Background(), cfg, zerolog.LevelDebugValue)
	require.NoError(t, err)
}

func TestStorage_PutObject(t *testing.T) {
	cfg := getCfg()
	st, err := NewStorage(context.Background(), cfg, zerolog.LevelDebugValue)
	require.NoError(t, err)
	buf := bytes.NewBuffer([]byte("1234567890"))
	err = st.PutObject(context.Background(), "/test.txt", buf)
	require.NoError(t, err)
	buf = bytes.NewBuffer([]byte("1234567890"))
	err = st.PutObject(context.Background(), "/testdb/test.txt", buf)
	require.NoError(t, err)
}

func TestStorage_GetObject(t *testing.T) {
	cfg := getCfg()
	st, err := NewStorage(context.Background(), cfg, zerolog.LevelDebugValue)
	require.NoError(t, err)
	obj, err := st.GetObject(context.Background(), "/test.txt")
	require.NoError(t, err)
	data, err := io.ReadAll(obj)
	require.NoError(t, err)
	bytes.Equal(data, []byte("1234567890"))
}

func TestStorage_Walking(t *testing.T) {
	cfg := getCfg()
	st, err := NewStorage(context.Background(), cfg, zerolog.LevelDebugValue)
	require.NoError(t, err)
	buf := bytes.NewBuffer([]byte("1234567890"))
	err = st.PutObject(context.Background(), "/test.txt", buf)
	require.NoError(t, err)
	buf = bytes.NewBuffer([]byte("1234567890"))
	err = st.PutObject(context.Background(), "/testdb/test.txt", buf)
	require.NoError(t, err)

	files, dirs, err := st.ListDir(context.Background())
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Len(t, dirs, 1)
	require.Equal(t, "test.txt", files[0])
	s3Dir := dirs[0].(*Storage)
	require.Equal(t, "testdb/", s3Dir.prefix)

	nextDir := dirs[0]
	files, dirs, err = nextDir.ListDir(context.Background())
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Len(t, dirs, 0)
	require.Equal(t, "test.txt", files[0])
}

func TestStorage_Delete(t *testing.T) {
	cfg := getCfg()
	st, err := NewStorage(context.Background(), cfg, zerolog.LevelDebugValue)
	require.NoError(t, err)

	buf := bytes.NewBuffer([]byte("1234567890"))
	err = st.PutObject(context.Background(), "/test_to_del.txt", buf)
	require.NoError(t, err)

	files, _, err := st.ListDir(context.Background())
	require.NoError(t, err)
	require.Contains(t, files, "test_to_del.txt")

	err = st.DeleteV2(context.Background(), "/test_to_del.txt")
	require.NoError(t, err)

	files, _, err = st.ListDir(context.Background())
	require.NoError(t, err)
	require.NotContains(t, files, "test_to_del.txt")
}
