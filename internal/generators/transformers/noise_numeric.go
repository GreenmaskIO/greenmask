package transformers

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/generators"
	"github.com/shopspring/decimal"
)

const (
	noiseNumericTransformerFloatSize = 8
	noiseNumericTransformerSignByte  = 1
)

type NoiseNumericLimiter struct {
	MinValue      decimal.Decimal
	MaxValue      decimal.Decimal
	precision     int32
	withPrecision bool
}

func NewNoiseNumericLimiter(minVal, maxVal decimal.Decimal) (*NoiseNumericLimiter, error) {
	if minVal.GreaterThanOrEqual(maxVal) {
		return nil, ErrWrongLimits
	}
	return &NoiseNumericLimiter{
		MinValue: minVal,
		MaxValue: maxVal,
	}, nil
}

func (ni *NoiseNumericLimiter) Limit(v decimal.Decimal) decimal.Decimal {
	if v.GreaterThan(ni.MaxValue) {
		return ni.MaxValue
	}
	if v.LessThan(ni.MinValue) {
		return ni.MinValue
	}
	if ni.withPrecision {
		return v.Round(ni.precision)
	}
	return v
}

func (ni *NoiseNumericLimiter) SetPrecision(v int32) {
	ni.precision = v
	ni.withPrecision = true
}

type NoiseNumericTransformer struct {
	generator  generators.Generator
	limiter    *NoiseNumericLimiter
	byteLength int
	minRatio   float64
	maxRatio   float64
}

func NewNoiseNumericTransformer(limiter *NoiseNumericLimiter, minRatio, maxRatio float64) *NoiseNumericTransformer {
	return &NoiseNumericTransformer{
		limiter:    limiter,
		byteLength: noiseNumericTransformerFloatSize + noiseNumericTransformerSignByte,
		minRatio:   minRatio,
		maxRatio:   maxRatio,
	}
}

func (nt *NoiseNumericTransformer) Transform(ctx context.Context, original decimal.Decimal) (decimal.Decimal, error) {
	var limiter = nt.limiter
	limiterAny := ctx.Value("limiter")

	if limiterAny != nil {
		limiter = limiterAny.(*NoiseNumericLimiter)
	}

	resBytes, err := nt.generator.Generate(original.BigInt().Bytes())
	if err != nil {
		return decimal.Decimal{}, err
	}

	randFloat := float64(int64(generators.BuildUint64FromBytes(resBytes[:8]))) / (1 << 63)
	if randFloat < 0 {
		randFloat = -randFloat
	}

	multiplayer := nt.minRatio + randFloat*(nt.maxRatio-nt.minRatio)

	negative := resBytes[8]%2 == 0
	if negative && multiplayer > 0 || !negative && multiplayer < 0 {
		multiplayer = -multiplayer
	}

	res := original.Add(original.Mul(decimal.NewFromFloat(multiplayer)))

	if limiter != nil {
		res = limiter.Limit(res)
	}

	return res, nil
}

func (nt *NoiseNumericTransformer) GetRequiredGeneratorByteLength() int {
	return nt.byteLength
}

func (nt *NoiseNumericTransformer) SetGenerator(g generators.Generator) error {
	if g.Size() < nt.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", nt.byteLength, g.Size())
	}
	nt.generator = g
	return nil
}
