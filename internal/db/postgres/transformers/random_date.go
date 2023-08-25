package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/transformers/utils"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const RandomDateTransformerName = "RandomDate"

var RandomDateTransformerMeta = utils.TransformerMeta{
	Description: "Generate random date",
	ParamsDescription: map[string]string{
		"min":      "min value",
		"max":      "max value",
		"truncate": "Truncate date till the part (year, month, day, hour, second, nano)",
		"useType":  "use another type instead column type",
		"nullable": "generate null value randomly (default false)",
		"fraction": "NULL value distribution within the table (default Fraction 10%)",
	},
	NewTransformer: NewRandomDateTransformer,
	Settings: utils.NewTransformerSettings().
		SetNullable().
		SetVariadic().
		SetCastVar(time.Time{}).
		SetSupportedOids(
			pgtype.DateOID,
			pgtype.TimestampOID,
			pgtype.TimestamptzOID,
		).
		SetName(RandomDateTransformerName),
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
	utils.TransformerBase
	RandomDateTransformerParams
	rand     *rand.Rand
	generate dateGeneratorFunc
	min      time.Time
	max      time.Time
}

func NewRandomDateTransformer(
	base *utils.TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	tParams := RandomDateTransformerParams{
		Fraction: utils.DefaultNullFraction,
	}

	if err := utils.ParseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
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

	if err := res.Scan(res.Min, &res.min); err != nil {
		return nil, fmt.Errorf("cannot decode min value: %w", err)
	}

	if err := res.Scan(res.Max, &res.max); err != nil {
		return nil, fmt.Errorf("cannot decode max value: %w", err)
	}

	return res, nil
}

func (rdt *RandomDateTransformer) TransformAttr(val string) (string, error) {
	if rdt.Nullable {
		if rdt.rand.Float32() < rdt.Fraction {
			return utils.DefaultNullSeq, nil
		}
	}
	resTime := rdt.generate(rdt.rand, &rdt.min, &rdt.max, &rdt.Truncate)
	res, err := rdt.EncodePlan.Encode(resTime, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}

func (rdt *RandomDateTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := utils.GetColumnValueFromCsvRecord(rdt.Table, data, rdt.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := rdt.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return utils.UpdateAttributeAndBuildRecord(rdt.Table, record, transformedAttr, rdt.ColumnNum)
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
