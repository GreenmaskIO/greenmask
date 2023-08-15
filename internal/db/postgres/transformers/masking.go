package transformers

import (
	"fmt"
	"math/rand"
	"time"

	masker "github.com/ggwhite/go-masker"
	"github.com/jackc/pgx/v5/pgtype"

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

const (
	MaskingTransformerName = "Masking"
)

var MaskingTransformerMeta = TransformerMeta{
	Description: `Masking with value passed through "value" parameter`,
	ParamsDescription: map[string]string{
		"value": "replacing value",
	},
	NewTransformer: NewMaskingTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetCastVar("").
		SetSupportedOids(
			pgtype.TextOID,
			pgtype.VarcharOID,
		).
		SetName(MaskingTransformerName),
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
	rand            *rand.Rand
	masker          *masker.Masker
	maskingFunction maskingFunction
}

func NewMaskingTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

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
		rand:                     rand.New(rand.NewSource(time.Now().UnixMicro())),
		masker:                   masker.New(),
		maskingFunction:          mf,
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	return res, nil
}

func (mt *MaskingTransformer) TransformAttr(val string) (string, error) {
	if val == DefaultNullSeq {
		return val, nil
	}
	if mt.Nullable {
		if mt.rand.Float32() < mt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	return mt.maskingFunction(val), nil
}

func (mt *MaskingTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := getColumnValueFromCsvRecord(mt.Table, data, mt.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := mt.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return updateAttributeAndBuildRecord(mt.Table, record, transformedAttr, mt.ColumnNum)
}
