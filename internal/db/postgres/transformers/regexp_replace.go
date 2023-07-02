package transformers

import (
	"fmt"
	"math/rand"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var RegexpReplaceTransformerMeta = TransformerMeta{
	Description: `RegexpReplace with value passed through "value" parameter`,
	ParamsDescription: map[string]string{
		"regexp":  "regular expression",
		"replace": "replacement including regexp groups",
	},
	NewTransformer: NewRegexpReplaceTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetVariadic().
		SetCastVar("").
		SetSupportedOids(
			pgtype.VarcharOID,
			pgtype.TextOID,
		),
}

type RegexpReplaceTransformerParams struct {
	Regexp   string  `mapstructure:"regexp" validate:"required"`
	Replace  string  `mapstructure:"replace" validate:"required"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RegexpReplaceTransformer struct {
	TransformerBase
	RegexpReplaceTransformerParams
	rand   *rand.Rand
	regexp *regexp.Regexp
}

func NewRegexpReplaceTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	tParams := RegexpReplaceTransformerParams{
		Fraction: DefaultNullFraction,
	}
	if err := parseTransformerParams(params, &tParams); err != nil {
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

func (rt *RegexpReplaceTransformer) Transform(val string) (string, error) {
	if val == DefaultNullSeq {
		return val, nil
	}
	if rt.Nullable {
		if rt.rand.Float32() < rt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	return rt.regexp.ReplaceAllString(val, rt.Replace), nil
}
