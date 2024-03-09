package transformers

import (
	"context"
	"errors"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/generators"
)

var (
	ErrWrongLimits = errors.New("wrong limits")
)

type Int64Limiter struct {
	MinValue         int64
	MaxValue         int64
	maxValueFromZero uint64
	// offset - the offset from zero
	offset int64
}

func NewInt64Limiter(minValue, maxValue int64) (*Int64Limiter, error) {
	if minValue >= maxValue {
		return nil, ErrWrongLimits
	}

	maxValueFromZero := uint64(maxValue)
	offset := minValue

	if minValue < 0 {
		maxValueFromZero = uint64(maxValue) + uint64(-minValue)
	} else if minValue > 0 {
		maxValueFromZero = uint64(maxValue - minValue)
	}

	return &Int64Limiter{
		MinValue:         minValue,
		MaxValue:         maxValue,
		maxValueFromZero: maxValueFromZero + 1,
		offset:           offset,
	}, nil
}

func (l *Int64Limiter) Limit(v uint64) int64 {
	return int64(v%l.maxValueFromZero) + l.offset
}

type Int64Transformer struct {
	generator  generators.Generator
	limiter    *Int64Limiter
	byteLength int
}

func NewInt64Transformer(limiter *Int64Limiter) (*Int64Transformer, error) {
	return &Int64Transformer{
		limiter: limiter,
	}, nil
}

func (ig *Int64Transformer) Transform(ctx context.Context, original []byte) (int64, error) {
	var res int64
	var limiter = ig.limiter
	limiterAny := ctx.Value("limiter")

	if limiterAny != nil {
		limiter = limiterAny.(*Int64Limiter)
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

func (ig *Int64Transformer) GetRequiredGeneratorByteLength() int {
	return ig.byteLength
}

func (ig *Int64Transformer) SetGenerator(g generators.Generator) error {
	if g.Size() < ig.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", ig.byteLength, g.Size())
	}
	ig.generator = g
	return nil
}
