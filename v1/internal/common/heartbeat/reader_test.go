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
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/testutils"
)

func mustJsonMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func TestReader_Read(t *testing.T) {
	// Test heartbeat reader using Storage mock
	// Test all cases

	// Test case 1: Status is terminateWithStatus
	// Test case 2: Status is in-progress
	// Test case 3: Status is invalid
	// Test case 4: storage read error

	type test struct {
		name      string
		heartbeat Heartbeat
	}

	tests := []test{
		{
			name: "Status is terminateWithStatus",
			heartbeat: Heartbeat{
				Status:    StatusDone,
				UpdatedAt: time.Now(),
			},
		},
		{
			name: "Status is in-progress",
			heartbeat: Heartbeat{
				Status:    StatusInProgress,
				UpdatedAt: time.Now(),
			},
		},
		{
			name: "Status is failed",
			heartbeat: Heartbeat{
				Status:    StatusFailed,
				UpdatedAt: time.Now(),
			},
		},
		{
			name: "Status is terminateWithStatus with old timestamp",
			heartbeat: Heartbeat{
				Status:    StatusDone,
				UpdatedAt: time.Now().Add(-100 * time.Minute), // Old timestamp
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cxt := context.Background()
			st := &testutils.StorageMock{}
			obj := testutils.NewReadWriteCloserMock()
			obj.On("Read", mock.Anything).
				Return(0, nil, mustJsonMarshal(tc.heartbeat))
			obj.On("Close").
				Return(nil)

			st.On("GetObject", mock.Anything, FileName).
				Return(obj, nil)

			r := NewReader(st)
			s, err := r.Read(cxt)
			require.NoError(t, err)
			require.Equal(t, tc.heartbeat.Status, s)
			st.AssertNumberOfCalls(t, "GetObject", 1)
			obj.AssertNumberOfCalls(t, "Read", 1)
			obj.AssertNumberOfCalls(t, "Close", 1)
		})
	}

	t.Run("Wrong status", func(t *testing.T) {
		t.Parallel()
		cxt := context.Background()
		st := &testutils.StorageMock{}
		obj := testutils.NewReadWriteCloserMock()
		heartbeat := Heartbeat{
			Status:    "unknown", // Invalid status
			UpdatedAt: time.Now(),
		}
		obj.On("Read", mock.Anything).
			Return(0, nil, mustJsonMarshal(heartbeat))
		obj.On("Close").
			Return(nil)

		st.On("GetObject", mock.Anything, FileName).
			Return(obj, nil)

		r := NewReader(st)
		_, err := r.Read(cxt)
		require.ErrorIs(t, err, errInvalidStatus)
		st.AssertNumberOfCalls(t, "GetObject", 1)
		obj.AssertNumberOfCalls(t, "Read", 1)
		obj.AssertNumberOfCalls(t, "Close", 1)
	})

	t.Run("Storage error", func(t *testing.T) {
		t.Parallel()
		cxt := context.Background()
		st := &testutils.StorageMock{}

		st.On("GetObject", mock.Anything, FileName).
			Return(nil, errors.New("some error"))

		r := NewReader(st)
		_, err := r.Read(cxt)
		require.ErrorContains(t, err, "get object")
	})
}
