package transformers

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/greenmaskio/greenmask/pkg/generators"
)

type NoiseTimestampLimiter struct {
	minDate *time.Time
	maxDate *time.Time
}

func NewNoiseTimestampLimiter(minDate, maxDate *time.Time) (*NoiseTimestampLimiter, error) {

	if minDate != nil && maxDate != nil && minDate.After(*maxDate) {
		return nil, ErrWrongLimits
	}

	return &NoiseTimestampLimiter{
		minDate: minDate,
		maxDate: maxDate,
	}, nil
}

func (ntl *NoiseTimestampLimiter) Limit(v time.Time) time.Time {
	if ntl.maxDate != nil && v.After(*ntl.maxDate) {
		return *ntl.maxDate
	}
	if ntl.minDate != nil && v.Before(*ntl.minDate) {
		return *ntl.minDate
	}
	return v
}

type NoiseTimestamp struct {
	byteLength int
	limiter    *NoiseTimestampLimiter
	generator  generators.Generator
	minRatio   int64
	maxRatio   int64
	truncater  *DateTruncater
	distance   int64
}

func NewNoiseTimestamp(minRatio, maxRatio time.Duration, truncatePart string, limiter *NoiseTimestampLimiter) (*NoiseTimestamp, error) {

	if minRatio >= maxRatio {
		return nil, ErrWrongLimits
	}

	var dt *DateTruncater
	var err error

	if truncatePart != "" {
		dt, err = NewDateTruncater(truncatePart)
		if err != nil {
			return nil, err
		}
	}

	return &NoiseTimestamp{
		truncater:  dt,
		limiter:    limiter,
		minRatio:   int64(minRatio),
		maxRatio:   int64(maxRatio),
		distance:   int64(maxRatio - minRatio),
		byteLength: 17, // 16 bytes for sec and nano, 1 byte for sign
	}, nil
}

func (d *NoiseTimestamp) Transform(l *NoiseTimestampLimiter, v time.Time) (time.Time, error) {
	limiter := d.limiter
	if l != nil {
		limiter = l
	}

	genBytes, err := d.generator.Generate([]byte(v.String()))
	if err != nil {
		return time.Time{}, fmt.Errorf("error generating noise timestamp: %w", err)
	}

	negative := genBytes[0]%2 == 0
	offset := int64(binary.LittleEndian.Uint64(genBytes[1:9])) % d.distance
	if offset < 0 {
		offset = -offset
	}
	// TODO: Consider how to add nanoseconds
	//nano := int64(binary.LittleEndian.Uint64(genBytes[9:]) % 1000000000)
	//if nano < 0 {
	//	nano = -nano
	//}

	sec := d.minRatio + offset

	if negative {
		v = v.Add(-time.Duration(sec))
	} else {
		v = v.Add(time.Duration(sec))
	}

	if limiter != nil {
		v = limiter.Limit(v)
	}

	if d.truncater != nil {
		v = d.truncater.Truncate(v)
	}

	return v, nil
}

func (d *NoiseTimestamp) GetRequiredGeneratorByteLength() int {
	return d.byteLength
}

func (d *NoiseTimestamp) SetGenerator(g generators.Generator) error {
	if g.Size() < d.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", d.byteLength, g.Size())
	}
	d.generator = g
	return nil
}
