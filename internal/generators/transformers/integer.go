package transformers

import (
	"context"
	"errors"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/generators"
)

var (
	ErrUnsupportedGeneratorLength = errors.New("unsupported generator byte length")
	ErrWrongLimits                = errors.New("wrong limits")
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
		maxValueFromZero: maxValueFromZero,
		offset:           offset,
	}, nil
}

func (l *Int64Limiter) Limit(v uint64) int64 {
	return int64(v%l.maxValueFromZero) + l.offset
}

type Int64Transformer struct {
	generator generators.Generator
	limiter   *Int64Limiter
}

func NewInt64Transformer(generator generators.Generator, limiter *Int64Limiter) (*Int64Transformer, error) {
	if generator.Size() != 8 {
		return nil, fmt.Errorf("expected 8 length but got %d: %w", generator.Size(), ErrUnsupportedGeneratorLength)
	}
	return &Int64Transformer{
		generator: generator,
		limiter:   limiter,
	}, nil
}

func (ig *Int64Transformer) Transform(ctx context.Context, original []byte) ([]byte, error) {
	var res int64

	limiter := ctx.Value("limiter").(*Int64Limiter)
	if limiter == nil && ig.limiter != nil {
		limiter = ig.limiter
	}

	resBytes, err := ig.generator.Generate(original)
	if err != nil {
		return nil, err
	}

	if limiter != nil {
		res = limiter.Limit(generators.BuildUint64FromBytes(resBytes))
	} else {
		res = generators.BuildInt64FromBytes(resBytes)
	}

	return []byte(fmt.Sprintf("%d", res)), nil
}
