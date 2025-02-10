package transformers

import (
	"fmt"
	"math"

	"github.com/greenmaskio/greenmask/pkg/generators"
)

type Float64Limiter struct {
	minValue  float64
	maxValue  float64
	precision int
}

func NewFloat64Limiter(minValue, maxValue float64, precision int) (*Float64Limiter, error) {
	if minValue >= maxValue {
		return nil, ErrWrongLimits
	}
	return &Float64Limiter{
		minValue:  minValue,
		maxValue:  maxValue,
		precision: precision,
	}, nil
}

func (fl *Float64Limiter) Limit(v float64) float64 {
	res := fl.minValue + v*(fl.maxValue-fl.minValue)
	return Round(fl.precision, res)
}

type RandomFloat64Transformer struct {
	byteLength int
	generator  generators.Generator
	limiter    *Float64Limiter
}

func NewRandomFloat64Transformer(limiter *Float64Limiter) *RandomFloat64Transformer {
	return &RandomFloat64Transformer{
		byteLength: 8,
		limiter:    limiter,
	}
}

func (f *RandomFloat64Transformer) GetRequiredGeneratorByteLength() int {
	return f.byteLength
}

func (f *RandomFloat64Transformer) SetGenerator(g generators.Generator) error {
	if g.Size() < f.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", f.byteLength, g.Size())
	}
	f.generator = g
	return nil
}

func (f *RandomFloat64Transformer) Transform(l *Float64Limiter, original []byte) (float64, error) {
	limiter := f.limiter
	if l != nil {
		limiter = l
	}

	resBytes, err := f.generator.Generate(original)
	if err != nil {
		return 0, err
	}

	res := float64(int64(generators.BuildUint64FromBytes(resBytes))) / (1 << 63)

	if res < 0 {
		res = -res
	}

	if limiter != nil {
		res = limiter.Limit(res)
	}
	return res, nil
}

func Round(precision int, num float64) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}
