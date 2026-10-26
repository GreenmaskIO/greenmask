package transformers

import (
	"fmt"
	"math"

	"github.com/greenmaskio/greenmask/internal/generators"
)

type Float64Limiter struct {
	minValue  float64
	maxValue  float64
	precision int
}

func NewFloat64Limiter(minValue, maxValue float64, precision int) (*Float64Limiter, error) {
	if minValue > maxValue {
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
	if math.IsInf(fl.maxValue-fl.minValue, 0) {
		// max-min overflows for wide ranges, so interpolate without it.
		res = fl.minValue*(1-v) + fl.maxValue*v
	}
	return clampRound(fl.precision, res, fl.minValue, fl.maxValue)
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

// maxExactFloat is 2^53, above it float64 has no fractional part and rounding is a no-op.
const maxExactFloat = 1 << 53

func Round(precision int, num float64) float64 {
	return roundWith(math.Round, precision, num)
}

func roundWith(f func(float64) float64, precision int, num float64) float64 {
	p := math.Pow(10, float64(precision))
	scaled := num * p
	if math.IsInf(scaled, 0) || math.IsNaN(scaled) || math.Abs(scaled) > maxExactFloat {
		return num
	}
	return f(scaled) / p
}

// clampRound clamps v into [minValue, maxValue] and rounds it without leaving the range.
func clampRound(precision int, v, minValue, maxValue float64) float64 {
	res := Round(precision, min(max(v, minValue), maxValue))
	if res > maxValue {
		res = roundWith(math.Floor, precision, maxValue)
	}
	if res < minValue {
		res = roundWith(math.Ceil, precision, minValue)
	}
	if res < minValue || res > maxValue {
		// No value with this precision fits into the range.
		return min(max(v, minValue), maxValue)
	}
	return res
}
