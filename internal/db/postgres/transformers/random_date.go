package transformers

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var truncateParts = []string{"year", "month", "day", "hour", "second", "millisecond", "microsecond", "nanosecond"}

var RandomDateTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"RandomDate",
		"Generate random date in the provided interval",
		toolkit.TupleTransformation,
	),

	NewRandomDateTransformer,

	toolkit.MustNewParameter(
		"column",
		"column name",
		new(string),
		nil,
	).SetIsColumn(toolkit.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("date", "timestamp", "timestamptz"),
	).SetRequired(true),

	toolkit.MustNewParameter(
		"min",
		"min threshold value of random value",
		nil,
		nil,
	).SetRequired(true).
		SetLinkParameter("column"),

	toolkit.MustNewParameter(
		"max",
		"max threshold value of random value",
		nil,
		nil,
	).SetRequired(true).
		SetLinkParameter("column"),

	toolkit.MustNewParameter(
		"truncate",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
		new(string),
		nil,
	),

	toolkit.MustNewParameter(
		"keepNull",
		"do not replace NULL values to random value",
		new(bool),
		New(true),
	),
)

type dateGeneratorFunc func(r *rand.Rand, startDate *time.Time, endDate *time.Time, truncate *string) time.Time

type RandomDateTransformerParams struct {
	Min      string  `mapstructure:"min" validate:"required"`
	Max      string  `mapstructure:"max" validate:"required"`
	Truncate string  `mapstructure:"truncate" validate:"omitempty,oneof=year month day hour second nano"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RandomDateTransformer struct {
	columnName string
	rand       *rand.Rand
	generate   dateGeneratorFunc
	min        *time.Time
	max        *time.Time
	truncate   string
	keepNull   bool
}

func NewRandomDateTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var columnName, truncate string
	var minTime, maxTime time.Time
	var generator dateGeneratorFunc = generateRandomTime
	var keepNull bool

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	p = parameters["min"]
	minTime, ok := p.Value().(time.Time)
	if !ok {
		return nil, nil, errors.New(`unexpected type for "min" parameter`)
	}

	p = parameters["max"]
	maxTime, ok = p.Value().(time.Time)
	if !ok {
		return nil, nil, errors.New(`unexpected type for "max" parameter`)
	}

	p = parameters["keepNull"]
	if err := p.Scan(&keepNull); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "keepNull" param: %w`, err)
	}

	p = parameters["truncate"]
	if err := p.Scan(&truncate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "truncate" param: %w`, err)
	}

	if truncate != "" {
		generator = generateRandomTimeTruncate
	}

	if minTime.After(maxTime) {
		return nil, toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				AddMeta("max", maxTime).
				AddMeta("min", minTime).
				SetMsg("max value must be greater than min"),
		}, nil
	}
	return &RandomDateTransformer{
		keepNull:   keepNull,
		truncate:   truncate,
		columnName: columnName,
		min:        &minTime,
		max:        &maxTime,
		generate:   generator,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil, nil

}

func (rdt *RandomDateTransformer) Init(ctx context.Context) error {
	return nil
}

func (rdt *RandomDateTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	if r.IsNull(rdt.columnName) && rdt.keepNull {
		return r, nil
	}

	res := rdt.generate(rdt.rand, rdt.min, rdt.max, &rdt.truncate)
	if err := r.SetAttribute(rdt.columnName, &res); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func generateRandomTime(r *rand.Rand, startDate *time.Time, endDate *time.Time, truncate *string) time.Time {
	delta := endDate.UnixMicro() - startDate.UnixMicro()
	return time.UnixMicro(r.Int63n(delta) + startDate.UnixMicro())
}

func generateRandomTimeTruncate(r *rand.Rand, startDate *time.Time, endDate *time.Time, truncate *string) time.Time {
	delta := endDate.UnixMicro() - startDate.UnixMicro()
	randVal := time.UnixMicro(r.Int63n(delta) + startDate.UnixMicro())
	return truncateDate(&randVal, truncate)
}

// TruncateDate - truncate date till the provided part of date
func truncateDate(t *time.Time, part *string) time.Time {
	var month time.Month = 1
	var day = 1
	var year, hour, minute, second, nano int
	switch *part {
	case "nano":
		nano = t.Nanosecond()
		fallthrough
	case "second":
		second = t.Second()
		fallthrough
	case "minute":
		minute = t.Minute()
		fallthrough
	case "hour":
		hour = t.Hour()
		fallthrough
	case "day":
		day = t.Day()
		fallthrough
	case "month":
		month = t.Month()
		fallthrough
	case "year":
		year = t.Year()
	default:
		panic(fmt.Sprintf(`wrong Truncate value "%s"`, *part))
	}
	return time.Date(year, month, day, hour, minute, second, nano,
		t.Location(),
	)
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RandomDateTransformerDefinition)
}
