package heartbeat

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/testutils"
)

func TestReader_Read(t *testing.T) {
	// Test heartbeat reader using Storage mock
	// Test all cases

	// Test case 1: Status is done
	// Test case 2: Status is in-progress
	// Test case 3: Status is invalid
	// Test case 4: storage read error

	type test struct {
		name   string
		status Status
	}

	tests := []test{
		{
			name:   "Status is done",
			status: StatusDone,
		},
		{
			name:   "Status is in-progress",
			status: StatusInProgress,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cxt := context.Background()
			st := &testutils.StorageMock{}
			obj := testutils.NewReadWriteCloserMock()
			obj.On("Read", mock.Anything).
				Return(0, nil, []byte(tc.status))
			obj.On("Close").
				Return(nil)

			st.On("GetObject", mock.Anything, heartBeatFileName).
				Return(obj, nil)

			r := NewReader(st)
			s, err := r.Read(cxt)
			require.NoError(t, err)
			require.Equal(t, tc.status, s)
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
		obj.On("Read", mock.Anything).
			Return(0, nil, []byte("unknown"))
		obj.On("Close").
			Return(nil)

		st.On("GetObject", mock.Anything, heartBeatFileName).
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

		st.On("GetObject", mock.Anything, heartBeatFileName).
			Return(nil, errors.New("some error"))

		r := NewReader(st)
		_, err := r.Read(cxt)
		require.ErrorContains(t, err, "get object")
	})
}
