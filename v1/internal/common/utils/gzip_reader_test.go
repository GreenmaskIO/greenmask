package utils

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/stretchr/testify/require"
)

type readCloserMock struct {
	*bytes.Buffer
	closeCallCount int
}

func (r *readCloserMock) Close() error {
	r.closeCallCount++
	return nil
}

func TestNewGzipReader_Read(t *testing.T) {
	data := `20383   24ca7574-0adb-4b17-8777-93f5589dbea2    2017-12-13 13:46:49.39
20384   d0d4a55c-7752-453e-8334-772a889fb917    2017-12-13 13:46:49.453
20385   ac8617aa-5a2d-4bb8-a9a5-ed879a4b33cd    2017-12-13 13:46:49.5
`
	buf := new(bytes.Buffer)
	gzData := gzip.NewWriter(buf)
	_, err := gzData.Write([]byte(data))
	require.NoError(t, err)
	err = gzData.Flush()
	require.NoError(t, err)
	err = gzData.Close()
	require.NoError(t, err)
	objSrc := &readCloserMock{Buffer: buf}
	r, err := NewGzipReader(objSrc, false)
	require.NoError(t, err)
	readBuf := make([]byte, 1024)
	n, err := r.Read(readBuf)
	require.NoError(t, err)
	require.Equal(t, []byte(data), readBuf[:n])
}

func TestNewGzipReader_Close(t *testing.T) {
	data := ""
	buf := new(bytes.Buffer)
	gzData := gzip.NewWriter(buf)
	_, err := gzData.Write([]byte(data))
	require.NoError(t, err)
	err = gzData.Flush()
	require.NoError(t, err)
	err = gzData.Close()
	require.NoError(t, err)
	objSrc := &readCloserMock{Buffer: buf, closeCallCount: 0}
	r, err := NewGzipReader(objSrc, false)
	require.NoError(t, err)
	err = r.Close()
	require.NoError(t, err)
	require.Equal(t, 1, objSrc.closeCallCount)
	gz := r.gz.(*gzip.Reader)
	_, err = gz.Read([]byte{})
	require.Error(t, err)
}
