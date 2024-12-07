package ioutils

import (
	"bytes"
	"compress/gzip"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type writeCloserMock struct {
	data           []byte
	writeCallCount int
	writeCallFunc  func(callCount int) error
	closeCallCount int
	closeCallFunc  func(callCount int) error
}

func (w *writeCloserMock) Write(p []byte) (n int, err error) {
	w.writeCallCount++
	if w.writeCallFunc != nil {
		return 0, w.writeCallFunc(w.writeCallCount)
	}
	w.data = append(w.data, p...)
	return len(p), nil
}

func (w *writeCloserMock) Close() error {
	w.closeCallCount++
	if w.closeCallFunc != nil {
		return w.closeCallFunc(w.closeCallCount)
	}
	return nil
}

func TestNewGzipWriter_Write(t *testing.T) {
	data := `20383   24ca7574-0adb-4b17-8777-93f5589dbea2    2017-12-13 13:46:49.39
20384   d0d4a55c-7752-453e-8334-772a889fb917    2017-12-13 13:46:49.453
20385   ac8617aa-5a2d-4bb8-a9a5-ed879a4b33cd    2017-12-13 13:46:49.5
`
	testDataBuf := new(bytes.Buffer)
	gzData := gzip.NewWriter(testDataBuf)
	_, err := gzData.Write([]byte(data))
	require.NoError(t, err)
	err = gzData.Flush()
	require.NoError(t, err)
	err = gzData.Close()
	require.NoError(t, err)
	expectedData := testDataBuf.Bytes()

	require.NoError(t, err)
	err = gzData.Close()
	require.NoError(t, err)
	objSrc := &writeCloserMock{}
	r := NewGzipWriter(objSrc, false)
	require.NoError(t, err)
	_, err = r.Write([]byte(data))
	require.NoError(t, err)
	err = r.Close()
	require.NoError(t, err)

	require.Equal(t, expectedData, objSrc.data)
}

func TestNewGzipWriter_Close(t *testing.T) {
	data := `20383   24ca7574-0adb-4b17-8777-93f5589dbea2    2017-12-13 13:46:49.39
20384   d0d4a55c-7752-453e-8334-772a889fb917    2017-12-13 13:46:49.453
20385   ac8617aa-5a2d-4bb8-a9a5-ed879a4b33cd    2017-12-13 13:46:49.5
`
	t.Run("Success", func(t *testing.T) {
		objSrc := &writeCloserMock{}
		r := NewGzipWriter(objSrc, false)
		err := r.Close()
		require.NoError(t, err)
		require.Equal(t, 1, objSrc.closeCallCount)
	})

	t.Run("Flush Error", func(t *testing.T) {
		objSrc := &writeCloserMock{
			writeCallFunc: func(c int) error {
				if c == 2 {
					return errors.New("storage object error")
				}
				return nil
			},
		}
		r := NewGzipWriter(objSrc, false)
		_, err := r.Write([]byte(data))
		require.NoError(t, err)

		err = r.Close()
		require.Error(t, err)
		require.ErrorContains(t, err, "error closing gzip writer")
		require.Equal(t, 1, objSrc.closeCallCount)
		require.Equal(t, 2, objSrc.writeCallCount)
	})

	t.Run("Storage object close Error", func(t *testing.T) {
		objSrc := &writeCloserMock{
			closeCallFunc: func(c int) error {
				return errors.New("storage object error")
			},
		}
		r := NewGzipWriter(objSrc, false)
		err := r.Close()
		require.Error(t, err)
		require.Equal(t, 1, objSrc.closeCallCount)
		require.ErrorContains(t, err, "error closing dump file")
	})
}
