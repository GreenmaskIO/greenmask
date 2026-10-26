package transformers

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/greenmaskio/greenmask/internal/generators"
)

const (
	randomNumericSlackBytes = 8
	// randomNumericMaxByteLength is the biggest output of the hash engine (sha3-512).
	randomNumericMaxByteLength = 64
)

type RandomNumericLimiter struct {
	MinValue decimal.Decimal
	MaxValue decimal.Decimal
}

func NewRandomNumericLimiter(minValue, maxValue decimal.Decimal) (*RandomNumericLimiter, error) {

	if minValue.GreaterThan(maxValue) {
		return nil, ErrWrongLimits
	}

	return &RandomNumericLimiter{
		MinValue: minValue,
		MaxValue: maxValue,
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

// scaledBounds returns min and max in units of 10^-precision, rounded inwards so results stay within bounds.
func (l *RandomNumericLimiter) scaledBounds(precision int32) (*big.Int, *big.Int) {
	return l.MinValue.Shift(precision).Ceil().BigInt(), l.MaxValue.Shift(precision).Floor().BigInt()
}

// Limit maps v into the inclusive range [MinValue, MaxValue] with the given number of decimals.
func (l *RandomNumericLimiter) Limit(v *big.Int, precision int32) decimal.Decimal {
	lo, hi := l.scaledBounds(precision)
	if hi.Cmp(lo) < 0 {
		// No value with this precision fits into the range.
		return l.MinValue
	}
	count := new(big.Int).Sub(hi, lo)
	count.Add(count, big.NewInt(1))
	res := new(big.Int).Mod(v, count)
	res.Add(res, lo)
	return decimal.NewFromBigInt(res, -precision)
}

// byteLength returns the generator bytes needed to cover the range with randomNumericSlackBytes to reduce bias.
func (l *RandomNumericLimiter) byteLength(precision int32) int {
	lo, hi := l.scaledBounds(precision)
	distance := new(big.Int).Sub(hi, lo)
	size := (distance.BitLen()+7)/8 + randomNumericSlackBytes
	return min(size, randomNumericMaxByteLength)
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

	return &RandomNumericTransformer{
		limiter:    limiter,
		byteLength: limiter.byteLength(precision),
		precision:  precision,
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
	var limiter = ig.limiter
	if ig.dynamicLimiter != nil {
		limiter = ig.dynamicLimiter
		ig.dynamicLimiter = nil
	}

	resBytes, err := ig.generator.Generate(original)
	if err != nil {
		return decimal.Decimal{}, err
	}

	v := new(big.Int).SetBytes(resBytes[:ig.byteLength])
	return limiter.Limit(v, ig.precision), nil
}
