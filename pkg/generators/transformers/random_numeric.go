package transformers

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/greenmaskio/greenmask/pkg/generators"
)

type RandomNumericLimiter struct {
	MinValue      decimal.Decimal
	MaxValue      decimal.Decimal
	distance      decimal.Decimal
	precision     int32
	withPrecision bool
}

func NewRandomNumericLimiter(minValue, maxValue decimal.Decimal) (*RandomNumericLimiter, error) {

	if minValue.GreaterThanOrEqual(maxValue) {
		return nil, ErrWrongLimits
	}

	return &RandomNumericLimiter{
		MinValue: minValue,
		MaxValue: maxValue,
		distance: maxValue.Sub(minValue),
	}, nil
}

func GetMinAndMaxNumericValueBySetting(digitsBeforeDecimal int) (decimal.Decimal, decimal.Decimal, error) {
	minDecimalStr := fmt.Sprintf("-%s", strings.Repeat("9", digitsBeforeDecimal))
	maxDecimalStr := strings.Repeat("9", digitsBeforeDecimal)
	minDecimal, err := decimal.NewFromString(minDecimalStr)
	if err != nil {
		return decimal.Decimal{}, decimal.Decimal{}, fmt.Errorf("error creating big decimal min threshold")
	}
	maxDecimal, err := decimal.NewFromString(maxDecimalStr)
	if err != nil {
		return decimal.Decimal{}, decimal.Decimal{}, fmt.Errorf("error creating big decimal max threshold")
	}
	return minDecimal, maxDecimal, nil
}

func NewRandomNumericLimiterFromSize(digitsBeforeDecimal int) (*RandomNumericLimiter, error) {
	minDecimal, maxDecimal, err := GetMinAndMaxNumericValueBySetting(digitsBeforeDecimal)
	if err != nil {
		return nil, err
	}
	return NewRandomNumericLimiter(minDecimal, maxDecimal)
}

func (l *RandomNumericLimiter) SetPrecision(v int32) {
	l.precision = v
	l.withPrecision = true
}

func (l *RandomNumericLimiter) ByteLength() int {
	minValueBitLength := l.MinValue.BigInt().BitLen()
	maxValueBitLength := l.MaxValue.BigInt().BitLen()
	if minValueBitLength > maxValueBitLength {
		return minValueBitLength
	}
	return maxValueBitLength
}

func (l *RandomNumericLimiter) Limit(v decimal.Decimal) decimal.Decimal {
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

type RandomNumericTransformer struct {
	generator      generators.Generator
	limiter        *RandomNumericLimiter
	dynamicLimiter *RandomNumericLimiter
	byteLength     int
	precision      int32
}

func NewRandomNumericTransformer(limiter *RandomNumericLimiter, precision int32) (*RandomNumericTransformer, error) {
	if limiter == nil {
		return nil, fmt.Errorf("limiter for BigInt values cannot be nil")
	}

	maxBytesLength := max(getByteByDecimal(limiter.MinValue), getByteByDecimal(limiter.MaxValue))

	return &RandomNumericTransformer{
		limiter:    limiter,
		byteLength: maxBytesLength,
		precision:  -precision,
	}, nil
}

func (ig *RandomNumericTransformer) GetRequiredGeneratorByteLength() int {
	return ig.byteLength
}

func (ig *RandomNumericTransformer) SetGenerator(g generators.Generator) error {
	if g.Size() < ig.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", ig.byteLength, g.Size())
	}
	ig.generator = g
	return nil
}

// SetDynamicLimiter sets the limiter for the dynamic mode. dynamicLimiter will be used set as nil after the Transform
// call.
func (ig *RandomNumericTransformer) SetDynamicLimiter(l *RandomNumericLimiter) *RandomNumericTransformer {
	if l == nil {
		panic("bug: limiter for RandomNumericTransformer values cannot be nil")
	}
	ig.dynamicLimiter = l
	return ig
}

func (ig *RandomNumericTransformer) Transform(original []byte) (decimal.Decimal, error) {
	var res decimal.Decimal
	var limiter = ig.limiter
	if ig.dynamicLimiter != nil {
		limiter = ig.dynamicLimiter
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

	if ig.dynamicLimiter != nil {
		limiter = nil
	}

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
