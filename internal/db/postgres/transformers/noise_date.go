package transformers

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

// TODO: Ensure pqinterval.Duration returns duration in int64 for date and time

var NoiseDateTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"NoiseDate",
		"Apply random noise for date values",
		toolkit.TupleTransformation,
	),

	NewNoiseDateTransformer,

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
		"ratio",
		"max random duration for noise",
		nil,
		nil,
	).SetRequired(true).
		SetCastDbType("interval"),

	toolkit.MustNewParameter(
		"truncate",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
		new(string),
		nil,
	),
)

type dateNoiseFunc func(r *rand.Rand, ration time.Duration, original *time.Time, truncate *string) time.Time

type NoiseDateTransformer struct {
	columnName string
	ratio      time.Duration
	ratioVal   any
	truncate   string
	rand       *rand.Rand
	generate   dateNoiseFunc
}

func NewNoiseDateTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var columnName, truncate string
	var ratio time.Duration
	var generator dateNoiseFunc = generateNoisedTime

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	p = parameters["ratio"]
	intervalValue, ok := p.Value().(pgtype.Interval)
	if !ok {
		return nil, nil, fmt.Errorf(`cannot cast "ratio" param to interval value`)
	}
	ratio = (time.Duration(intervalValue.Days) * time.Hour * 24) +
		(time.Duration(intervalValue.Months) * 12 * 24 * time.Hour) +
		(time.Duration(intervalValue.Microseconds) * time.Millisecond)

	p = parameters["truncate"]
	if err := p.Scan(&truncate); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "truncate" param: %w`, err)
	}

	if truncate != "" {
		generator = generateNoisedTimeTruncate
	}

	return &NoiseDateTransformer{
		columnName: columnName,
		ratio:      ratio / time.Microsecond,
		ratioVal:   intervalValue,
		truncate:   truncate,
		rand:       rand.New(rand.NewSource(time.Now().UnixMicro())),
		generate:   generator,
	}, nil, nil
}

func (ndt *NoiseDateTransformer) Init(ctx context.Context) error {
	return nil
}

func (ndt *NoiseDateTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	if r.IsNull(ndt.columnName) {
		return r, nil
	}

	val, err := r.GetAttribute(ndt.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}
	timeVal, ok := val.(time.Time)
	if !ok {
		return nil, errors.New("cannot cast to time.Time")
	}
	resTime := ndt.generate(ndt.rand, ndt.ratio, &timeVal, &ndt.truncate)
	if err := r.SetAttribute(ndt.columnName, resTime); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func generateNoisedTime(r *rand.Rand, ratio time.Duration, val *time.Time, truncate *string) time.Time {
	return time.UnixMicro(val.UnixMicro() + r.Int63n(int64(ratio)))
}

func generateNoisedTimeTruncate(r *rand.Rand, ratio time.Duration, val *time.Time, truncate *string) time.Time {
	randVal := time.UnixMicro(val.UnixMicro() + r.Int63n(int64(ratio)))
	return truncateDate(&randVal, truncate)
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(NoiseDateTransformerDefinition)
}
