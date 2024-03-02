package random

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
)

const (
	Int8Mode = iota
	Int16Mode
	Int32Mode
	Int64Mode
	UInt8Mode
	UInt16Mode
	UInt32Mode
	UInt64Mode
)

var ErrIntLimitsOutOfRange = errors.New("integer limits out of range")

type Limits struct {
	MinValue int64
	MaxValue int64
}

type Int struct {
	r      *rand.Rand
	size   int
	limits *Limits
}

func NewInt(seed int64, mode int, limits *Limits) (*Int, error) {
	switch mode {
	case Int8Mode, Int16Mode, Int32Mode, Int64Mode, UInt8Mode, UInt16Mode, UInt32Mode, UInt64Mode:
	default:
		return nil, fmt.Errorf("unknown generator mode %d", mode)
	}

	var size int
	switch mode {
	case Int8Mode, UInt8Mode:
		size = 1
	case Int16Mode, UInt16Mode:
		size = 2
	case Int32Mode, UInt32Mode:
		size = 4
	case Int64Mode, UInt64Mode:
		size = 8
	}

	if limits == nil {
		switch mode {
		case Int8Mode:
			limits = &Limits{
				MinValue: math.MinInt8,
				MaxValue: math.MaxInt8,
			}
		case Int16Mode:
			limits = &Limits{
				MinValue: math.MinInt16,
				MaxValue: math.MaxInt16,
			}
		case Int32Mode:
			limits = &Limits{
				MinValue: math.MinInt32,
				MaxValue: math.MaxInt32,
			}
		case Int64Mode:
			limits = &Limits{
				MinValue: math.MinInt64,
				MaxValue: math.MaxInt64,
			}
		case UInt8Mode:
			limits = &Limits{
				MinValue: 0,
				MaxValue: math.MaxUint8,
			}
		case UInt16Mode:
			limits = &Limits{
				MinValue: 0,
				MaxValue: math.MaxUint16,
			}
		case UInt32Mode:
			limits = &Limits{
				MinValue: 0,
				MaxValue: math.MaxUint32,
			}
		case UInt64Mode:
			limits = &Limits{
				MinValue: 0,
				MaxValue: math.MaxUint64,
			}
		}

	}

	if limits != nil {
		switch mode {
		case Int8Mode:
			if limits.MinValue < math.MinInt8 || limits.MaxValue > math.MaxInt8 {
				return nil, ErrIntLimitsOutOfRange
			}

		case Int16Mode:
			if limits.MinValue < math.MinInt16 || limits.MaxValue > math.MaxInt16 {
				return nil, ErrIntLimitsOutOfRange
			}

		case Int32Mode:
			if limits.MinValue < math.MinInt32 || limits.MaxValue > math.MaxInt32 {
				return nil, ErrIntLimitsOutOfRange
			}

		case Int64Mode:
			if limits.MinValue < math.MinInt64 || limits.MaxValue > math.MaxInt64 {
				return nil, ErrIntLimitsOutOfRange
			}

		case UInt8Mode:
			if limits.MinValue < 0 || limits.MaxValue > math.MaxUint8 {
				return nil, ErrIntLimitsOutOfRange
			}

		case UInt16Mode:
			if limits.MinValue < 0 || limits.MaxValue > math.MaxUint16 {
				return nil, ErrIntLimitsOutOfRange
			}

		case UInt32Mode:
			if limits.MinValue < 0 || limits.MaxValue > math.MaxUint32 {
				return nil, ErrIntLimitsOutOfRange
			}

		case UInt64Mode:
			if limits.MinValue < 0 || limits.MaxValue > math.MaxUint64 {
				return nil, ErrIntLimitsOutOfRange
			}

		}
	}

	return &Int{
		r:      rand.New(rand.NewSource(seed)),
		size:   size,
		limits: limits,
	}, nil
}

func (i *Int) GetInt64() int64 {

	if i.limits.MinValue < 0 && i.limits.MaxValue <= 0 {
		minValue := i.limits.MaxValue * -1
		maxValue := i.limits.MinValue * -1
		return (i.r.Int63n(maxValue-minValue) + minValue) * -1
	} else if i.limits.MinValue < 0 && i.limits.MaxValue > 0 {
		// The period [min:max] must be > 0 and has the same length in point between max and min
		// for instance len([-10:10]) = 20. We generate random and subtract offset from 0 to the max.
		// For ex rand(20) = 2 then the result 2-10 = -8
		maxValue := (i.limits.MinValue * -1) + i.limits.MaxValue
		return i.r.Int63n(maxValue) - i.limits.MaxValue
	}

	return i.r.Int63n(i.limits.MaxValue-i.limits.MinValue) + i.limits.MinValue
}

func (i *Int) Generate(data []byte) ([]byte, error) {
	value := i.GetInt64()

	res := make([]byte, i.size)
	for shift := 0; shift < i.size; shift++ {
		res[shift] = byte((value << (8 * shift)) & 0xFF)
	}
	return res, nil
}

func (i *Int) Size() int {
	return i.size
}
