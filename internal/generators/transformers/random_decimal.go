package transformers

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/greenmaskio/greenmask/internal/generators"
)

type DecimalLimiter struct {
	MinValue      decimal.Decimal
	MaxValue      decimal.Decimal
	distance      decimal.Decimal
	precision     int32
	withPrecision bool
}

func NewDecimalLimiter(minValue, maxValue decimal.Decimal) (*DecimalLimiter, error) {

	if minValue.GreaterThanOrEqual(maxValue) {
		return nil, ErrWrongLimits
	}

	return &DecimalLimiter{
		MinValue: minValue,
		MaxValue: maxValue,
		distance: maxValue.Sub(minValue),
	}, nil
}

func NewDecimalLimiterFromSize(digitsBeforeDecimal int) (*DecimalLimiter, error) {
	minDecimalStr := fmt.Sprintf("-%s", strings.Repeat("9", digitsBeforeDecimal))
	maxDecimalStr := fmt.Sprintf("%s", strings.Repeat("9", digitsBeforeDecimal))
	minDecimal, err := decimal.NewFromString(minDecimalStr)
	if err != nil {
		return nil, fmt.Errorf("error creating big decimal min threshold")
	}
	maxDecimal, err := decimal.NewFromString(maxDecimalStr)
	if err != nil {
		return nil, fmt.Errorf("error creating big decimal max threshold")
	}
	return NewDecimalLimiter(minDecimal, maxDecimal)
}

func (l *DecimalLimiter) SetPrecision(v int32) *DecimalLimiter {
	l.precision = v
	l.withPrecision = true
	return l
}

func (l *DecimalLimiter) ByteLength() int {
	minValueBitLength := l.MinValue.BigInt().BitLen()
	maxValueBitLength := l.MaxValue.BigInt().BitLen()
	if minValueBitLength > maxValueBitLength {
		return minValueBitLength
	}
	return maxValueBitLength
}

func (l *DecimalLimiter) Limit(v decimal.Decimal) decimal.Decimal {
	res := l.MinValue.Add(v.Mod(l.distance))
	if res.GreaterThan(l.MaxValue) {
		res = res.Mod(l.MaxValue)
	} else if res.LessThan(l.MinValue) {
		res = res.Mod(l.MinValue)
	}
	if l.withPrecision {
		res = res.Round(l.precision)
	}
	return res
}

type RandomDecimalTransformer struct {
	generator  generators.Generator
	limiter    *DecimalLimiter
	byteLength int
	precision  int32
}

func NewRandomDecimalTransformer(limiter *DecimalLimiter, precision int32) (*RandomDecimalTransformer, error) {
	if limiter == nil {
		return nil, fmt.Errorf("limiter for BigInt values cannot be nil")
	}

	maxBytesLength := max(getByteByDecimal(limiter.MinValue), getByteByDecimal(limiter.MaxValue))

	return &RandomDecimalTransformer{
		limiter:    limiter,
		byteLength: maxBytesLength,
		precision:  -precision,
	}, nil
}

func (ig *RandomDecimalTransformer) GetRequiredGeneratorByteLength() int {
	return ig.byteLength
}

func (ig *RandomDecimalTransformer) SetGenerator(g generators.Generator) error {
	if g.Size() < ig.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", ig.byteLength, g.Size())
	}
	ig.generator = g
	return nil
}

func (ig *RandomDecimalTransformer) Transform(ctx context.Context, original []byte) (decimal.Decimal, error) {
	var res decimal.Decimal
	var limiter = ig.limiter
	limiterAny := ctx.Value("limiter")

	if limiterAny != nil {
		limiter = limiterAny.(*DecimalLimiter)
	}

	resBytes, err := ig.generator.Generate(original)
	if err != nil {
		return decimal.Decimal{}, err
	}

	res = newDecimalFromBytes(resBytes[:ig.byteLength], ig.precision)
	if res.LessThan(decimal.NewFromInt(0)) {
		res = res.Mul(decimal.NewFromInt(-1))
	}
	res = limiter.Limit(res)

	return res, nil
}

func getByteByDecimal(v decimal.Decimal) int {
	bitLen := v.BigInt().BitLen()
	if bitLen%8 > 0 {
		return bitLen/8 + 1
	}
	return bitLen / 8
}

func newDecimalFromBytes(data []byte, exp int32) decimal.Decimal {
	v := new(big.Int).SetBytes(data)
	return decimal.NewFromBigInt(v, exp)
}
