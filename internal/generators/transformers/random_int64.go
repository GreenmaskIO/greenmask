package transformers

import (
	"errors"
	"fmt"
	"math"

	"github.com/greenmaskio/greenmask/internal/generators"
)

var (
	ErrWrongLimits = errors.New("wrong limits")
)

type Int64Limiter struct {
	MinValue int64
	MaxValue int64
	// distance is max - min computed in uint64 so the full int64 range does not overflow.
	distance uint64
}

func NewInt64Limiter(minValue, maxValue int64) (*Int64Limiter, error) {
	if minValue > maxValue {
		return nil, ErrWrongLimits
	}

	return &Int64Limiter{
		MinValue: minValue,
		MaxValue: maxValue,
		distance: uint64(maxValue) - uint64(minValue),
	}, nil
}

// Limit maps v into the inclusive range [MinValue, MaxValue].
func (l *Int64Limiter) Limit(v uint64) int64 {
	if l.distance == math.MaxUint64 {
		return int64(v + uint64(l.MinValue))
	}
	return int64(uint64(l.MinValue) + v%(l.distance+1))
}

type RandomInt64Transformer struct {
	generator  generators.Generator
	limiter    *Int64Limiter
	byteLength int
}

func NewRandomInt64Transformer(limiter *Int64Limiter, size int) (*RandomInt64Transformer, error) {
	return &RandomInt64Transformer{
		limiter:    limiter,
		byteLength: size,
	}, nil
}

func (ig *RandomInt64Transformer) Transform(l *Int64Limiter, original []byte) (int64, error) {
	var res int64
	limiter := ig.limiter
	if l != nil {
		limiter = l
	}

	resBytes, err := ig.generator.Generate(original)
	if err != nil {
		return 0, err
	}

	if limiter != nil {
		res = limiter.Limit(generators.BuildUint64FromBytes(resBytes))
	} else {
		res = generators.BuildInt64FromBytes(resBytes)
	}

	return res, nil
}

func (ig *RandomInt64Transformer) GetRequiredGeneratorByteLength() int {
	return ig.byteLength
}

func (ig *RandomInt64Transformer) SetGenerator(g generators.Generator) error {
	if g.Size() < ig.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", ig.byteLength, g.Size())
	}
	ig.generator = g
	return nil
}
