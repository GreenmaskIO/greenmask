package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

// TODO: Test this transformer

var RandomDateTransformerSupportedOids = []int{
	pgtype.DateOID,
	pgtype.TimestampOID,
	pgtype.TimestamptzOID,
}

var RandomDateTransformerMeta = TransformerMeta{
	Description: "Generate random date",
	ParamsDescription: map[string]string{
		"min":      "min value",
		"max":      "max value",
		"truncate": "Truncate date till the part (year, month, day, hour, second, nano)",
		"useType":  "use another type instead column type",
		"nullable": "generate null value randomly (default false)",
		"fraction": "NULL value distribution within the table (default Fraction 10%)",
	},
	SupportedTypeOids: RandomDateTransformerSupportedOids,
	NewTransformer:    NewRandomDateTransformer,
}

var truncateParts = []string{"year", "month", "day", "hour", "second", "millisecond", "microsecond", "nanosecond"}

type dateGeneratorFunc func(r *rand.Rand, startDate *time.Time, endDate *time.Time, truncate *string) time.Time

type RandomDateTransformerParams struct {
	Min      string  `mapstructure:"min" validate:"required"`
	Max      string  `mapstructure:"max" validate:"required"`
	Truncate string  `mapstructure:"truncate" validate:"omitempty,oneof=year month day hour second nano"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RandomDateTransformer struct {
	TransformerBase
	RandomDateTransformerParams
	rand     *rand.Rand
	generate dateGeneratorFunc
	min      time.Time
	max      time.Time
}

func NewRandomDateTransformer(
	column pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	useType string,
	params map[string]interface{},
) (domains.Transformer, error) {

	base, err := NewTransformerBase(column, typeMap, useType, RandomDateTransformerSupportedOids, time.Time{})
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}

	tParams := RandomDateTransformerParams{
		Fraction: DefaultNullFraction,
	}

	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable on not null column")
	}

	res := &RandomDateTransformer{
		TransformerBase:             *base,
		RandomDateTransformerParams: tParams,
		rand:                        rand.New(rand.NewSource(time.Now().UnixMicro())),
		generate:                    generateRandomTime,
	}

	if res.Truncate != "" {
		res.generate = generateRandomTimeTruncate
	}

	if err = res.Scan(res.Min, &res.min); err != nil {
		return nil, fmt.Errorf("cannot decode min value: %w", err)
	}

	if err = res.Scan(res.Max, &res.max); err != nil {
		return nil, fmt.Errorf("cannot decode max value: %w", err)
	}

	return res, nil
}

func (gtt *RandomDateTransformer) Transform(val string) (string, error) {
	if gtt.Nullable {
		if gtt.rand.Float32() < gtt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	resTime := gtt.generate(gtt.rand, &gtt.min, &gtt.max, &gtt.Truncate)
	res, err := gtt.EncodePlan.Encode(resTime, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
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
