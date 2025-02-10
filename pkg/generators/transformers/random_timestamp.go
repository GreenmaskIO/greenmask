package transformers

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/greenmaskio/greenmask/pkg/generators"
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

const TimestampTransformerByteLength = 16

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

type timestampThreshold struct {
	sec  int64
	nano int64
}

type TimestampLimiter struct {
	minDate          *timestampThreshold
	maxDate          *timestampThreshold
	maxValueFromZero uint64
	offset           int64
}

func NewTimestampLimiter(minDate, maxDate time.Time) (*TimestampLimiter, error) {

	if minDate.After(maxDate) {
		return nil, ErrWrongLimits
	}

	minDateThreshold := &timestampThreshold{
		sec:  minDate.Unix(),
		nano: int64(minDate.Nanosecond()) + 1,
	}
	maxDateThreshold := &timestampThreshold{
		sec:  maxDate.Unix(),
		nano: int64(maxDate.Nanosecond()) + 1,
	}

	maxValueFromZero := uint64(maxDateThreshold.sec)
	offset := minDateThreshold.sec

	if minDateThreshold.sec < 0 {
		if maxDateThreshold.sec < 0 {
			maxValueFromZero = uint64(-minDateThreshold.sec) - uint64(-maxDateThreshold.sec)
		} else {
			maxValueFromZero = uint64(maxDateThreshold.sec) + uint64(-minDateThreshold.sec)
		}
	} else if minDateThreshold.sec > 0 {
		maxValueFromZero = uint64(maxDateThreshold.sec - minDateThreshold.sec)
	}

	return &TimestampLimiter{
		minDate:          minDateThreshold,
		maxDate:          maxDateThreshold,
		maxValueFromZero: maxValueFromZero + 1,
		offset:           offset,
	}, nil
}

func (dl *TimestampLimiter) Limit(sec, nano int64) (int64, int64) {
	sec = (sec % int64(dl.maxValueFromZero)) + dl.offset
	if sec == dl.minDate.sec {
		nano = nano % dl.minDate.nano
	} else if sec == dl.maxDate.sec {
		nano = nano % dl.maxDate.nano
	}
	return sec, nano
}

type Timestamp struct {
	truncater  *DateTruncater
	generator  generators.Generator
	byteLength int
	limiter    *TimestampLimiter
}

func NewRandomTimestamp(truncatePart string, limiter *TimestampLimiter) (*Timestamp, error) {
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
		truncater:  dt,
		limiter:    limiter,
		byteLength: TimestampTransformerByteLength,
	}, nil
}

func (d *Timestamp) Transform(l *TimestampLimiter, data []byte) (time.Time, error) {
	limiter := d.limiter
	if l != nil {
		limiter = l
	}

	genBytes, err := d.generator.Generate(data)
	if err != nil {
		return time.Time{}, err
	}

	sec := int64(binary.LittleEndian.Uint64(genBytes[:8]))
	nano := int64(binary.LittleEndian.Uint64(genBytes[8:]) % 1000000000)

	if sec < 0 {
		sec = -sec
	}
	if nano < 0 {
		nano = -nano
	}

	var res time.Time

	if limiter != nil {
		sec, nano = limiter.Limit(sec, nano)
		res = time.Unix(sec, nano)
	} else {
		res = time.Unix(sec, nano)
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
