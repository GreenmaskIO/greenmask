package transformers

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/GreenmaskIO/greenmask/pkg/toolkit/transformers"
)

var truncateParts = []string{"year", "month", "day", "hour", "second", "millisecond", "microsecond", "nanosecond"}

var RandomDateTransformerDefinition = transformers.NewDefinition(
	transformers.MustNewTransformerProperties(
		"RandomDate",
		"Generate random date in the provided interval",
		transformers.TupleTransformation,
	),
	NewRandomDateTransformer,
	transformers.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(transformers.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("date", "timestamp", "timestamptz"),
		).SetRequired(true),
	transformers.MustNewParameter(
		"min",
		"min threshold value of random value",
		nil,
		nil,
	).SetRequired(true).
		SetLinkParameter("column"),
	transformers.MustNewParameter(
		"max",
		"max threshold value of random value",
		nil,
		nil,
	).SetRequired(true).
		SetLinkParameter("column"),
	transformers.MustNewParameter(
		"truncate",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
		new(string),
		nil,
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
}

func NewRandomDateTransformer(ctx context.Context, driver *transformers.Driver, parameters map[string]*transformers.Parameter) (transformers.Transformer, transformers.ValidationWarnings, error) {
	var columnName, truncate string
	var minTime, maxTime time.Time
	var generator dateGeneratorFunc = generateRandomTime

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

	p = parameters["truncate"]
	if err := p.Scan(&truncate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "truncate" param: %w`, err)
	}

	if truncate != "" {
		generator = generateRandomTimeTruncate
	}

	if minTime.After(maxTime) {
		return nil, transformers.ValidationWarnings{
			transformers.NewValidationWarning().
				AddMeta("max", maxTime).
				AddMeta("min", minTime).
				SetMsg("max value must be greater than min"),
		}, nil
	}
	return &RandomDateTransformer{
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

func (rdt *RandomDateTransformer) Validate(ctx context.Context) (transformers.ValidationWarnings, error) {
	return nil, nil
}

func (rdt *RandomDateTransformer) Transform(ctx context.Context, r *transformers.Record) (*transformers.Record, error) {
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
	DefaultTransformerRegistry.MustRegister(RandomDateTransformerDefinition)
}
