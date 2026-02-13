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

package heartbeat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type heartBeatWriterMock struct {
	mock.Mock
}

func (hbw *heartBeatWriterMock) Write(ctx context.Context, data Status) error {
	args := hbw.Called(ctx, data)
	return args.Error(0)
}

func TestWorker_Run(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		hbw := &heartBeatWriterMock{}
		ctx := context.Background()
		hbw.On("Write", mock.Anything, StatusDone).
			Return(nil)
		hbw.On("Write", mock.Anything, StatusInProgress).
			Return(nil)

		w := NewWorker(hbw)
		w.SetInterval(200 * time.Millisecond)
		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(w.Run(ctx))
		wg.Go(func() error {
			select {
			case <-ctx.Done():
				require.NoError(t, ctx.Err())
			case <-time.After(1 * time.Second):
				w.Terminate(StatusDone)
			}
			return nil
		})

		err := wg.Wait()
		require.NoError(t, err)
		hbw.AssertCalled(t, "Write", mock.Anything, StatusDone)
		hbw.AssertCalled(t, "Write", mock.Anything, StatusInProgress)
	})

	t.Run("context is cancelled", func(t *testing.T) {
		t.Parallel()
		// This wil not write StatusDone as context is cancelled
		hbw := &heartBeatWriterMock{}
		ctx, cancel := context.WithCancel(context.Background())
		hbw.On("Write", mock.Anything, StatusDone).
			Return(nil)
		hbw.On("Write", mock.Anything, StatusInProgress).
			Return(nil)

		w := NewWorker(hbw)
		w.SetInterval(500 * time.Millisecond)
		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(w.Run(ctx))
		wg.Go(func() error {
			select {
			case <-ctx.Done():
				require.NoError(t, ctx.Err())
			case <-time.After(1 * time.Second):
				cancel()
			}
			return nil
		})

		err := wg.Wait()
		require.NoError(t, err)
		hbw.AssertCalled(t, "Write", mock.Anything, StatusInProgress)
	})
}
