package utils

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type timeoutReader struct {
	mock.Mock
}

func (t *timeoutReader) Read(p []byte) (n int, err error) {
	args := t.Called(p)
	return args.Int(0), args.Error(1)
}

func TestDefaultContextReader_ReadContext(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		_, _ = buf.Write([]byte("hello world"))
		reader := NewDefaultContextReader(buf)

		p := make([]byte, 11)
		n, err := reader.ReadContext(context.Background(), p)
		require.NoError(t, err)
		assert.Equal(t, n, 11)
		assert.Equal(t, "hello world", string(p[:n]))
	})
	t.Run("timeout", func(t *testing.T) {
		r := new(timeoutReader)
		r.On("Read", mock.Anything).Run(func(args mock.Arguments) {
			time.Sleep(1 * time.Second)
		})
		reader := NewDefaultContextReader(r)
		p := make([]byte, 11)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := reader.ReadContext(ctx, p)
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestDefaultContextReader_SetContext(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		_, _ = buf.Write([]byte("hello world"))
		reader := NewDefaultContextReader(buf)

		p := make([]byte, 11)
		n, err := reader.WithContext(context.Background()).Read(p)
		require.NoError(t, err)
		assert.Equal(t, n, 11)
		assert.Equal(t, "hello world", string(p[:n]))
	})
	t.Run("timeout", func(t *testing.T) {
		r := new(timeoutReader)
		r.On("Read", mock.Anything).Run(func(args mock.Arguments) {
			time.Sleep(1 * time.Second)
		})
		reader := NewDefaultContextReader(r)
		p := make([]byte, 11)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := reader.WithContext(ctx).Read(p)
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
