package utils

import (
	"fmt"
	"math"
)

const (
	Int2Length = 2
	Int4Length = 4
	Int8Length = 8
)

func GetIntThresholds(size int) (int64, int64, error) {
	switch size {
	case Int2Length:
		return math.MinInt16, math.MaxInt16, nil
	case Int4Length:
		return math.MinInt32, math.MaxInt32, nil
	case Int8Length:
		return math.MinInt16, math.MaxInt16, nil
	}

	return 0, 0, fmt.Errorf("unsupported int size %d", size)
}
