package transformers

import (
	"fmt"
	"math/rand"
	"time"

	masker "github.com/ggwhite/go-masker"
	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const (
	MPassword   string = "password"
	MName       string = "name"
	MAddress    string = "addr"
	MEmail      string = "email"
	MMobile     string = "mobile"
	MTelephone  string = "tel"
	MID         string = "id"
	MCreditCard string = "credit"
	MURL        string = "url"
)

var MaskingTransformerSupportedOids = []int{
	pgtype.TextOID,
	pgtype.VarcharOID,
}

var MaskingTransformerMeta = TransformerMeta{
	Description: `Masking with value passed through "value" parameter`,
	ParamsDescription: map[string]string{
		"value": "replacing value",
	},
	SupportedTypeOids: MaskingTransformerSupportedOids,
	NewTransformer:    NewMaskingTransformer,
}

type maskingFunction func(val string) string

type MaskingTransformerParams struct {
	Type     string  `mapstructure:"type" validate:"required,oneof=password name addr email mobile tel id credit url"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type MaskingTransformer struct {
	TransformerBase
	MaskingTransformerParams
	Column          pgDomains.ColumnMeta
	rand            *rand.Rand
	masker          *masker.Masker
	maskingFunction maskingFunction
}

func NewMaskingTransformer(
	column pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	useType string,
	params map[string]interface{},
) (domains.Transformer, error) {
	base, err := NewTransformerBase(column, typeMap, useType, MaskingTransformerSupportedOids, "")
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}

	tParams := MaskingTransformerParams{
		Fraction: DefaultNullFraction,
	}
	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	m := masker.New()
	var mf maskingFunction
	switch tParams.Type {
	case MPassword:
		mf = m.Password
	case MName:
		mf = m.Name
	case MAddress:
		mf = m.Address
	case MEmail:
		mf = m.Email
	case MMobile:
		mf = m.Mobile
	case MID:
		mf = m.ID
	case MTelephone:
		mf = m.Telephone
	case MCreditCard:
		mf = m.CreditCard
	case MURL:
		mf = m.URL
	default:
		return nil, fmt.Errorf("wrong type: %s", tParams.Type)
	}

	res := &MaskingTransformer{
		TransformerBase:          *base,
		MaskingTransformerParams: tParams,
		Column:                   column,
		rand:                     rand.New(rand.NewSource(time.Now().UnixMicro())),
		masker:                   masker.New(),
		maskingFunction:          mf,
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	return res, nil
}

func (rt *MaskingTransformer) Transform(val string) (string, error) {
	if val == DefaultNullSeq {
		return val, nil
	}
	if rt.Nullable {
		if rt.rand.Float32() < rt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	return rt.maskingFunction(val), nil
}
