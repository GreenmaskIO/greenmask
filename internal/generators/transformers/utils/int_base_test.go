package utils

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetIntThresholds(t *testing.T) {
	tests := []struct {
		name        string
		size        int
		expectedMin int64
		expectedMax int64
	}{
		{
			name:        "with size 2",
			size:        Int2Length,
			expectedMin: math.MinInt16,
			expectedMax: math.MaxInt16,
		},
		{
			name:        "with size 4",
			size:        Int4Length,
			expectedMin: math.MinInt32,
			expectedMax: math.MaxInt32,
		},
		{
			name:        "with size 8",
			size:        Int8Length,
			expectedMin: math.MinInt16,
			expectedMax: math.MaxInt16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			minTreshhold, maxThreshhold, err := GetIntThresholds(tt.size)
			require.NoError(t, err)
			require.Equal(t, minTreshhold, tt.expectedMin)
			require.Equal(t, maxThreshhold, tt.expectedMax)
		})
	}
}
