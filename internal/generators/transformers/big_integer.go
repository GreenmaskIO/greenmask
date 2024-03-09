package transformers

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/greenmaskio/greenmask/internal/generators"
)

type BigIntLimiter struct {
	MinValue         decimal.Decimal
	MaxValue         decimal.Decimal
	maxValueFromZero decimal.Decimal
	// offset - the offset from zero
	offset decimal.Decimal
}

func NewBigIntLimiter(minValue, maxValue decimal.Decimal) (*BigIntLimiter, error) {

	if minValue.GreaterThanOrEqual(maxValue) {
		return nil, ErrWrongLimits
	}

	maxValueFromZero := maxValue
	offset := minValue

	if minValue.LessThan(decimal.NewFromInt(0)) {
		maxValueFromZero = maxValue.Add(decimal.NewFromInt(-1).Mul(minValue)) //uint64(maxValue) + uint64(-minValue)
	} else if minValue.GreaterThan(decimal.NewFromInt(0)) /* minValue > 0*/ {
		//maxValueFromZero = uint64(maxValue - minValue)
		maxValueFromZero = maxValue.Add(minValue)
	}

	return &BigIntLimiter{
		MinValue:         minValue,
		MaxValue:         maxValue,
		maxValueFromZero: maxValueFromZero,
		offset:           offset,
	}, nil
}

func NewBigIntLimiterFromSize(digitsBeforeDecimal int) (*BigIntLimiter, error) {
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
	return NewBigIntLimiter(minDecimal, maxDecimal)
}

func (l *BigIntLimiter) ByteLength() int {
	minValueBitLength := l.MinValue.BigInt().BitLen()
	maxValueBitLength := l.MaxValue.BigInt().BitLen()
	if minValueBitLength > maxValueBitLength {
		return minValueBitLength
	}
	return maxValueBitLength
}

func (l *BigIntLimiter) Limit(v decimal.Decimal) decimal.Decimal {
	return v.Mod(l.maxValueFromZero).Add(l.offset)
}

type BigIntTransformer struct {
	generator  generators.Generator
	limiter    *BigIntLimiter
	byteLength int
}

func NewBigIntTransformer(limiter *BigIntLimiter) (*BigIntTransformer, error) {
	if limiter == nil {
		return nil, fmt.Errorf("limiter for BigInt values cannot be nil")
	}

	maxBytesLength := max(getByteByDecimal(limiter.MinValue), getByteByDecimal(limiter.MaxValue))

	return &BigIntTransformer{
		limiter:    limiter,
		byteLength: maxBytesLength,
	}, nil
}

func (ig *BigIntTransformer) GetRequiredGeneratorByteLength() int {
	return ig.byteLength
}

func (ig *BigIntTransformer) SetGenerator(g generators.Generator) error {
	if g.Size() < ig.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", ig.byteLength, g.Size())
	}
	ig.generator = g
	return nil
}

func (ig *BigIntTransformer) Transform(ctx context.Context, original []byte) (decimal.Decimal, error) {
	var res decimal.Decimal
	var limiter = ig.limiter
	limiterAny := ctx.Value("limiter")

	if limiterAny != nil {
		limiter = limiterAny.(*BigIntLimiter)
	}

	resBytes, err := ig.generator.Generate(original)
	if err != nil {
		return decimal.Decimal{}, err
	}

	res = newDecimalFromBytes(resBytes[:ig.byteLength])
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

func newDecimalFromBytes(data []byte) decimal.Decimal {
	v := new(big.Int).SetBytes(data)
	return decimal.NewFromBigInt(v, 0)
}
