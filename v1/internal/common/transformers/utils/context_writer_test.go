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

type timeoutWriter struct {
	mock.Mock
}

func (t *timeoutWriter) Write(p []byte) (n int, err error) {
	args := t.Called(p)
	return args.Int(0), args.Error(1)
}

func TestDefaultContextWriter_WriteContext(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		writer := NewDefaultContextWriter(buf)

		p := []byte("hello world")
		n, err := writer.WriteContext(context.Background(), p)
		require.NoError(t, err)
		assert.Equal(t, n, 11)
		assert.Equal(t, "hello world", buf.String())
	})
	t.Run("timeout", func(t *testing.T) {
		w := new(timeoutWriter)
		w.On("Write", mock.Anything).Run(func(args mock.Arguments) {
			time.Sleep(1 * time.Second)
		})
		reader := NewDefaultContextWriter(w)
		p := make([]byte, 11)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := reader.WriteContext(ctx, p)
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestDefaultContextWriter_SetContext(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		reader := NewDefaultContextWriter(buf)

		p := []byte("hello world")
		n, err := reader.WithContext(context.Background()).Write(p)
		require.NoError(t, err)
		assert.Equal(t, n, 11)
		assert.Equal(t, "hello world", buf.String())
	})
	t.Run("timeout", func(t *testing.T) {
		w := new(timeoutWriter)
		w.On("Write", mock.Anything).Run(func(args mock.Arguments) {
			time.Sleep(1 * time.Second)
		})
		reader := NewDefaultContextWriter(w)
		p := make([]byte, 11)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := reader.WithContext(ctx).Write(p)
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
