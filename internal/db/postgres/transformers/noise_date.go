package transformers

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
)

// TODO: Ensure pqinterval.Duration returns duration in int64 for date and time

var NoiseDateTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"NoiseDate",
		"Apply random noise for date values",
	),

	NewNoiseDateTransformer,

	toolkit2.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit2.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("date", "timestamp", "timestamptz"),
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"ratio",
		"max random duration for noise",
	).SetRequired(true).
		SetCastDbType("interval"),

	toolkit2.MustNewParameter(
		"truncate",
		fmt.Sprintf("truncate date till the part (%s)", strings.Join(truncateParts, ", ")),
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

func NewNoiseDateTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {
	var columnName, truncate string
	var ratio time.Duration
	var generator dateNoiseFunc = generateNoisedTime

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	p = parameters["ratio"]
	v, err := p.Value()
	if err != nil {
		return nil, nil, fmt.Errorf(`error parsing "ratio" parameter`)
	}
	intervalValue, ok := v.(pgtype.Interval)
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

func (ndt *NoiseDateTransformer) Done(ctx context.Context) error {
	return nil
}

func (ndt *NoiseDateTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
	val, err := r.GetAttribute(ndt.columnName)
	if err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if val.IsNull {
		return r, nil
	}

	timeVal, ok := val.Value.(time.Time)
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
