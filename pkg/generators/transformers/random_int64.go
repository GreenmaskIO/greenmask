package transformers

import (
	"errors"
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/generators"
)

var (
	ErrWrongLimits = errors.New("wrong limits")
)

type Int64Limiter struct {
	MinValue int64
	MaxValue int64
	distance uint64
}

func NewInt64Limiter(minValue, maxValue int64) (*Int64Limiter, error) {
	if minValue >= maxValue {
		return nil, ErrWrongLimits
	}

	return &Int64Limiter{
		MinValue: minValue,
		MaxValue: maxValue,
		distance: uint64(maxValue - minValue),
	}, nil
}

func (l *Int64Limiter) Limit(v uint64) int64 {
	res := l.MinValue + int64(v%l.distance)
	if res < 0 {
		return res % l.MinValue
	}
	return res % l.MaxValue
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
