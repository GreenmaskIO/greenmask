package transformers

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/greenmaskio/greenmask/internal/generators"
)

const (
	YearTruncateName        = "year"
	MonthTruncateName       = "month"
	DayTruncateName         = "day"
	HourTruncateName        = "hour"
	MinuteTruncateName      = "minute"
	SecondTruncateName      = "second"
	MillisecondTruncateName = "millisecond"
	MicrosecondTruncateName = "microsecond"
	NanosecondTruncateName  = "nanosecond"
)

const (
	YearTruncateValue = iota + 1
	MonthTruncateValue
	DayTruncateValue
	HourTruncateValue
	MinuteTruncateValue
	SecondTruncateValue
	MillisecondTruncateValue
	MicrosecondTruncateValue
	NanosecondTruncateValue
)

type DateTruncater struct {
	part int
}

func NewDateTruncater(truncatePartName string) (*DateTruncater, error) {
	var part int
	var err error

	if truncatePartName != "" {
		part, err = getTruncatePartValueByName(truncatePartName)
		if err != nil {
			return nil, err
		}
	}

	return &DateTruncater{
		part: part,
	}, nil
}

func (dt *DateTruncater) Truncate(t time.Time) time.Time {
	var month time.Month = 1
	var day = 1
	var year, hour, minute, second, nano int
	switch dt.part {
	// TODO: Add MicrosecondTruncateValue, MillisecondTruncateValue truncate
	case NanosecondTruncateValue, MicrosecondTruncateValue, MillisecondTruncateValue:
		nano = t.Nanosecond()
		fallthrough
	case SecondTruncateValue:
		second = t.Second()
		fallthrough
	case MinuteTruncateValue:
		minute = t.Minute()
		fallthrough
	case HourTruncateValue:
		hour = t.Hour()
		fallthrough
	case DayTruncateValue:
		day = t.Day()
		fallthrough
	case MonthTruncateValue:
		month = t.Month()
		fallthrough
	case YearTruncateValue:
		year = t.Year()
	}
	res := time.Date(year, month, day, hour, minute, second, nano,
		t.Location(),
	)
	return res
}

func getTruncatePartValueByName(truncateName string) (truncate int, err error) {
	switch truncateName {
	case NanosecondTruncateName:
		truncate = NanosecondTruncateValue
	case MicrosecondTruncateName:
		truncate = MicrosecondTruncateValue
	case MillisecondTruncateName:
		truncate = MillisecondTruncateValue
	case SecondTruncateName:
		truncate = SecondTruncateValue
	case MinuteTruncateName:
		truncate = MinuteTruncateValue
	case HourTruncateName:
		truncate = HourTruncateValue
	case DayTruncateName:
		truncate = DayTruncateValue
	case MonthTruncateName:
		truncate = MonthTruncateValue
	case YearTruncateName:
		truncate = YearTruncateValue
	default:
		return 0, fmt.Errorf("unknown truncate part %s", truncateName)

	}
	return
}

type DateLimiter struct {
	minDate time.Time
	maxDate time.Time
}

func NewDateLimiter(minDate, maxDate time.Time) *DateLimiter {
	minDate.Unix()
	return &DateLimiter{}
}

func (dl *DateLimiter) Limit(sec, nano int64) time.Time {
	return time.Unix(sec, nano)
}

type Timestamp struct {
	truncater  *DateTruncater
	generator  generators.Generator
	byteLength int
	limiter    *DateLimiter
}

func NewTimestamp(truncatePart string, limiter *DateLimiter) (*Timestamp, error) {
	// var month time.Month = 1
	//	var day = 1
	//	var year, month, day, hour, minute, second, nano int
	//  var nano int64

	// year - 4
	// month, day, hour, minute, second - 1 * 5
	// nano - 8

	var dt *DateTruncater
	var err error

	if truncatePart != "" {
		dt, err = NewDateTruncater(truncatePart)
		if err != nil {
			return nil, err
		}
	}

	return &Timestamp{
		truncater: dt,
		limiter:   limiter,
	}, nil
}

func (d *Timestamp) Transform(ctx context.Context, data []byte) (time.Time, error) {
	genBytes, err := d.generator.Generate(data)
	if err != nil {
		return time.Time{}, err
	}

	sec := binary.LittleEndian.Uint64(genBytes[:8])
	nano := binary.LittleEndian.Uint64(genBytes[9:]) % 1000000000

	res := time.Unix(int64(sec), int64(nano))

	if d.limiter != nil {
		d.limiter
	}

	if d.truncater != nil {
		res = d.truncater.Truncate(res)
	}

	return res, err
}

func (d *Timestamp) GetRequiredGeneratorByteLength() int {
	return d.byteLength
}

func (d *Timestamp) SetGenerator(g generators.Generator) error {
	if g.Size() < d.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", d.byteLength, g.Size())
	}
	d.generator = g
	return nil
}
