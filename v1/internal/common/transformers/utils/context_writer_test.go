// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
