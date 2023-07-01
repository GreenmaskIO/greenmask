package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sanyokbig/pqinterval"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

// TODO: Ensure pqinterval.Duration returns duration in int64 for date and time

var NoiseDateTransformerSupportedOids = []int{
	pgtype.DateOID,
	pgtype.TimestampOID,
	pgtype.TimestamptzOID,
}

type dateNoiseFunc func(r *rand.Rand, ration time.Duration, original *time.Time, truncate *string) time.Time

var NoiseDateTransformerMeta = TransformerMeta{
	Description: "Generate random date",
	ParamsDescription: map[string]string{
		"ratio":    "max random duration for noise",
		"truncate": "Truncate date till the part (year, month, day, hour, second, nano)",
		"useType":  "use another type instead column type",
		"nullable": "generate null value randomly (default false)",
		"fraction": "NULL value distribution within the table (default Fraction 10%)",
	},
	SupportedTypeOids: NoiseDateTransformerSupportedOids,
	NewTransformer:    NewNoiseDateTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetVariadic(),
}

type NoiseDateTransformerParams struct {
	Ratio    string  `mapstructure:"ratio" validate:"required"`
	Truncate string  `mapstructure:"truncate" validate:"omitempty,oneof=year month day hour second nano"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type NoiseDateTransformer struct {
	TransformerBase
	NoiseDateTransformerParams
	rand     *rand.Rand
	generate dateNoiseFunc
	ratio    time.Duration
	val      time.Time
}

func NewNoiseDateTransformer(
	table *pgDomains.TableMeta,
	column *pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	params map[string]interface{},
) (domains.Transformer, error) {

	base, err := NewTransformerBase(table, column, NoiseDateTransformerMeta.Settings, params, typeMap, NoiseDateTransformerSupportedOids, time.Time{})
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}

	tParams := NoiseDateTransformerParams{
		Fraction: DefaultNullFraction,
	}

	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	intervalRatio := pqinterval.Interval{}
	if err := intervalRatio.Scan(tParams.Ratio); err != nil {
		return nil, fmt.Errorf("cannot scan ratio: %w", err)
	}

	ratio, err := intervalRatio.Duration()
	if err != nil {
		return nil, fmt.Errorf("cannot scan ratio: %w", err)
	}

	res := &NoiseDateTransformer{
		TransformerBase:            *base,
		NoiseDateTransformerParams: tParams,
		rand:                       rand.New(rand.NewSource(time.Now().UnixMicro())),
		generate:                   generateNoisedTime,
		ratio:                      ratio / time.Microsecond,
	}

	if res.Truncate != "" {
		res.generate = generateNoisedTimeTruncate
	}

	return res, nil
}

func (gtt *NoiseDateTransformer) Transform(val string) (string, error) {
	if val == DefaultNullSeq {
		return val, nil
	}
	if err := gtt.Scan(val, &gtt.val); err != nil {
		return "", fmt.Errorf("cannot scan string into time.Time: %w", err)
	}

	if gtt.Nullable {
		if gtt.rand.Float32() < gtt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	resTime := gtt.generate(gtt.rand, gtt.ratio, &gtt.val, &gtt.Truncate)
	res, err := gtt.EncodePlan.Encode(resTime, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}

func generateNoisedTime(r *rand.Rand, ratio time.Duration, val *time.Time, truncate *string) time.Time {
	return time.UnixMicro(val.UnixMicro() + r.Int63n(int64(ratio)))
}

func generateNoisedTimeTruncate(r *rand.Rand, ratio time.Duration, val *time.Time, truncate *string) time.Time {
	randVal := time.UnixMicro(val.UnixMicro() + r.Int63n(int64(ratio)))
	return truncateDate(&randVal, truncate)
}
