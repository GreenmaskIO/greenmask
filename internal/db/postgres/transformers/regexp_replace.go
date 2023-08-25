package transformers

import (
	"fmt"
	"math/rand"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/transformers/utils"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const RegexpReplaceTransformerName = "RegexpReplace"

var RegexpReplaceTransformerMeta = utils.TransformerMeta{
	Description: `RegexpReplace with value passed through "value" parameter`,
	ParamsDescription: map[string]string{
		"regexp":  "regular expression",
		"replace": "replacement including regexp groups",
	},
	NewTransformer: NewRegexpReplaceTransformer,
	Settings: utils.NewTransformerSettings().
		SetNullable().
		SetVariadic().
		SetCastVar("").
		SetSupportedOids(
			pgtype.VarcharOID,
			pgtype.TextOID,
			pgtype.QCharOID,
			pgtype.BPCharOID,
		).
		SetName(RegexpReplaceTransformerName),
}

type RegexpReplaceTransformerParams struct {
	Regexp   string  `mapstructure:"regexp" validate:"required"`
	Replace  string  `mapstructure:"replace" validate:"required"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RegexpReplaceTransformer struct {
	utils.TransformerBase
	RegexpReplaceTransformerParams
	rand   *rand.Rand
	regexp *regexp.Regexp
}

func NewRegexpReplaceTransformer(
	base *utils.TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	tParams := RegexpReplaceTransformerParams{
		Fraction: utils.DefaultNullFraction,
	}
	if err := utils.ParseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	re, err := regexp.Compile(tParams.Regexp)
	if err != nil {
		return nil, fmt.Errorf("cannot compile regular expression: %w", err)
	}

	res := &RegexpReplaceTransformer{
		TransformerBase:                *base,
		RegexpReplaceTransformerParams: tParams,
		rand:                           rand.New(rand.NewSource(time.Now().UnixMicro())),
		regexp:                         re,
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	return res, nil
}

func (rrt *RegexpReplaceTransformer) TransformAttr(val string) (string, error) {
	if val == utils.DefaultNullSeq {
		return val, nil
	}
	if rrt.Nullable {
		if rrt.rand.Float32() < rrt.Fraction {
			return utils.DefaultNullSeq, nil
		}
	}
	return rrt.regexp.ReplaceAllString(val, rrt.Replace), nil
}

func (rrt *RegexpReplaceTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := utils.GetColumnValueFromCsvRecord(rrt.Table, data, rrt.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := rrt.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return utils.UpdateAttributeAndBuildRecord(rrt.Table, record, transformedAttr, rrt.ColumnNum)
}
