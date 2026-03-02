package validate

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutAndGetObject(t *testing.T) {
	st := New("/")

	content := []byte("hello world")
	err := st.PutObject(context.Background(), "test.txt", bytes.NewReader(content))
	require.NoError(t, err)

	reader, err := st.GetObject(context.Background(), "test.txt")
	require.NoError(t, err)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content, data)
}

func TestExists(t *testing.T) {
	st := New("/")

	ok, err := st.Exists(context.Background(), "test.txt")
	require.NoError(t, err)
	assert.False(t, ok)

	err = st.PutObject(context.Background(), "test.txt", bytes.NewReader([]byte("data")))
	require.NoError(t, err)

	ok, err = st.Exists(context.Background(), "test.txt")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestDelete(t *testing.T) {
	st := New("/")
	err := st.PutObject(context.Background(), "to_delete.txt", bytes.NewReader([]byte("data")))
	require.NoError(t, err)

	err = st.Delete(context.Background(), "to_delete.txt")
	require.NoError(t, err)

	ok, err := st.Exists(context.Background(), "to_delete.txt")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestDeleteAll(t *testing.T) {
	st := New("/base")
	err := st.PutObject(context.Background(), "sub/one.txt", bytes.NewReader([]byte("1")))
	require.NoError(t, err)
	err = st.PutObject(context.Background(), "sub/two.txt", bytes.NewReader([]byte("2")))
	require.NoError(t, err)
	err = st.PutObject(context.Background(), "other/three.txt", bytes.NewReader([]byte("3")))
	require.NoError(t, err)

	err = st.DeleteAll(context.Background(), "sub")
	require.NoError(t, err)

	ok, _ := st.Exists(context.Background(), "sub/one.txt")
	assert.False(t, ok)
	ok, _ = st.Exists(context.Background(), "sub/two.txt")
	assert.False(t, ok)
	ok, _ = st.Exists(context.Background(), "other/three.txt")
	assert.True(t, ok)
}

func TestListDir(t *testing.T) {
	st := New("/")
	err := st.PutObject(context.Background(), "file1.txt", bytes.NewReader([]byte("a")))
	require.NoError(t, err)
	err = st.PutObject(context.Background(), "dir1/file2.txt", bytes.NewReader([]byte("b")))
	require.NoError(t, err)

	files, dirs, err := st.ListDir(context.Background())
	require.NoError(t, err)
	assert.Contains(t, files, "file1.txt")
	assert.Len(t, dirs, 1)
}

func TestStat(t *testing.T) {
	st := New("/")
	data := []byte("some-data")
	err := st.PutObject(context.Background(), "info.txt", bytes.NewReader(data))
	require.NoError(t, err)

	stat, err := st.Stat("info.txt")
	assert.NoError(t, err)
	assert.WithinDuration(t, time.Now(), stat.LastModified, time.Second*1)
}

func TestSubStorage(t *testing.T) {
	st := New("/")
	sub := st.SubStorage("subdir", true)

	err := sub.PutObject(context.Background(), "deep.txt", bytes.NewReader([]byte("deep-data")))
	require.NoError(t, err)

	ok, err := st.Exists(context.Background(), "deep.txt")
	require.NoError(t, err)
	assert.True(t, ok)
}
