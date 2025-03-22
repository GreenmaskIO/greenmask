package heartbeat

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/testutils"
)

func TestWriter_Write(t *testing.T) {
	// Test heartbeat reader using Storage mock
	// Test all cases

	// Test case 1: Status is done
	// Test case 2: Status is in-progress
	// Test case 3: Status is invalid
	// Test case 4: storage read error

	type test struct {
		name         string
		status       Status
		bytesWritten int
	}

	tests := []test{
		{
			name:         "Status is done",
			status:       StatusDone,
			bytesWritten: 4,
		},
		{
			name:         "Status is in-progress",
			status:       StatusInProgress,
			bytesWritten: 11,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cxt := context.Background()
			st := &testutils.StorageMock{}
			st.On("PutObject", mock.Anything, heartBeatFileName, mock.Anything).
				Run(func(args mock.Arguments) {
					body := args.Get(2).(io.Reader)
					data, err := io.ReadAll(body) // Read the data from the reader
					require.NoError(t, err)
					require.Equal(t, string(tt.status), string(data), "body should match expected content")
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
		st.On("PutObject", mock.Anything, heartBeatFileName, mock.Anything).
			Return(errors.New("some err"))

		r := NewWriter(st)
		err := r.Write(cxt, StatusDone)
		require.Error(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 1)
	})
}
