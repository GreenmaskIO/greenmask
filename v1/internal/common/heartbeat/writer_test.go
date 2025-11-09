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
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/testutils"
)

func TestWriter_Write(t *testing.T) {
	// Test heartbeat reader using Storage mock
	// Test all cases

	// Test case 1: Status is terminateWithStatus
	// Test case 2: Status is in-progress
	// Test case 3: Status is invalid
	// Test case 4: storage read error

	type test struct {
		name     string
		status   Status
		expected Heartbeat
	}

	tests := []test{
		{
			name:   "Status is terminateWithStatus",
			status: StatusDone,
			expected: Heartbeat{
				Status:    StatusDone,
				UpdatedAt: time.Now(),
			},
		},
		{
			name:   "Status is in-progress",
			status: StatusInProgress,
			expected: Heartbeat{
				Status:    StatusInProgress,
				UpdatedAt: time.Now(),
			},
		},
		{
			name:   "Status is failed",
			status: StatusFailed,
			expected: Heartbeat{
				Status:    StatusFailed,
				UpdatedAt: time.Now(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cxt := context.Background()
			st := &testutils.StorageMock{}
			st.On("PutObject", mock.Anything, FileName, mock.Anything).
				Run(func(args mock.Arguments) {
					body := args.Get(2).(io.Reader)
					actual := Heartbeat{}
					err := json.NewDecoder(body).Decode(&actual)
					require.NoError(t, err)
					require.Equal(t, tt.expected.Status, actual.Status)
					require.WithinDuration(t, tt.expected.UpdatedAt, actual.UpdatedAt, 1*time.Second)
				}).
				Return(nil)

			r := NewWriter(st)
			err := r.Write(cxt, tt.status)
			require.NoError(t, err)
			require.Equal(t, tt.status, tt.status)
			st.AssertNumberOfCalls(t, "PutObject", 1)
		})
	}

	t.Run("Wrong status", func(t *testing.T) {
		cxt := context.Background()
		st := &testutils.StorageMock{}
		r := NewWriter(st)
		err := r.Write(cxt, "unknown status")
		require.ErrorIs(t, err, errInvalidStatus)
		st.AssertNumberOfCalls(t, "PutObject", 0)
	})

	t.Run("Storage error", func(t *testing.T) {
		cxt := context.Background()
		st := &testutils.StorageMock{}
		st.On("PutObject", mock.Anything, FileName, mock.Anything).
			Return(errors.New("some err"))

		r := NewWriter(st)
		err := r.Write(cxt, StatusDone)
		require.Error(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 1)
	})
}
