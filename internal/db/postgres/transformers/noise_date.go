package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sanyokbig/pqinterval"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

// TODO: Ensure pqinterval.Duration returns duration in int64 for date and time

const NoiseDateTransformerName = "NoiseDate"

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
	NewTransformer: NewNoiseDateTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetCastVar(time.Time{}).
		SetVariadic().
		SetSupportedOids(
			pgtype.DateOID,
			pgtype.TimestampOID,
			pgtype.TimestamptzOID,
		).
		SetName(NoiseDateTransformerName),
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
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

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

func (ndt *NoiseDateTransformer) TransformAttr(val string) (string, error) {
	if val == DefaultNullSeq {
		return val, nil
	}
	if err := ndt.Scan(val, &ndt.val); err != nil {
		return "", fmt.Errorf("cannot scan string into time.Time: %w", err)
	}

	if ndt.Nullable {
		if ndt.rand.Float32() < ndt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	resTime := ndt.generate(ndt.rand, ndt.ratio, &ndt.val, &ndt.Truncate)
	res, err := ndt.EncodePlan.Encode(resTime, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}

func (ndt *NoiseDateTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := getColumnValueFromCsvRecord(ndt.Table, data, ndt.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := ndt.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return updateAttributeAndBuildRecord(ndt.Table, record, transformedAttr, ndt.ColumnNum)
}

func generateNoisedTime(r *rand.Rand, ratio time.Duration, val *time.Time, truncate *string) time.Time {
	return time.UnixMicro(val.UnixMicro() + r.Int63n(int64(ratio)))
}

func generateNoisedTimeTruncate(r *rand.Rand, ratio time.Duration, val *time.Time, truncate *string) time.Time {
	randVal := time.UnixMicro(val.UnixMicro() + r.Int63n(int64(ratio)))
	return truncateDate(&randVal, truncate)
}
