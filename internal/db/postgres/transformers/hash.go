package transformers

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/crypto/scrypt"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/transformers/utils"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

// TODO: Make length truncation

const (
	saltLength          = 32
	HashTransformerName = "Hash"
)

var HashTransformerMeta = utils.TransformerMeta{
	Description: `Replace with value passed through "value" parameter`,
	ParamsDescription: map[string]string{
		"salt": "secret salt that uses for applying hash function",
	},
	NewTransformer: NewHashTransformer,
	Settings: utils.NewTransformerSettings().
		SetNullable().
		SetVariadic().
		SetCastVar("").
		SetSupportedOids(
			pgtype.VarcharOID,
			pgtype.TextOID,
		).
		SetName(HashTransformerName),
}

type HashTransformerParams struct {
	Salt     string  `mapstructure:"salt"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type HashTransformer struct {
	utils.TransformerBase
	HashTransformerParams
	rand *rand.Rand
	salt []byte
}

func NewHashTransformer(
	base *utils.TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {
	tParams := HashTransformerParams{
		Fraction: utils.DefaultNullFraction,
	}
	if err := utils.ParseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	res := &HashTransformer{
		TransformerBase:       *base,
		HashTransformerParams: tParams,
		rand:                  rand.New(rand.NewSource(time.Now().UnixMicro())),
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	if res.Salt != "" {
		res.salt = []byte(res.Salt)
	} else {
		b := make([]byte, saltLength)
		if _, err := crand.Read(b); err != nil {
			return nil, err
		}
		res.salt = b
	}

	return res, nil
}

func (ht *HashTransformer) TransformAttr(data string) (string, error) {
	if data == utils.DefaultNullSeq {
		return data, nil
	}
	if ht.Nullable {
		if ht.rand.Float32() < ht.Fraction {
			return utils.DefaultNullSeq, nil
		}
	}
	dk, err := scrypt.Key([]byte(data), ht.salt, 32768, 8, 1, 32)
	if err != nil {
		return "", fmt.Errorf("cannot generate hash by value %w", err)
	}
	return base64.StdEncoding.EncodeToString(dk), nil
}

func (ht *HashTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := utils.GetColumnValueFromCsvRecord(ht.Table, data, ht.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := ht.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return utils.UpdateAttributeAndBuildRecord(ht.Table, record, transformedAttr, ht.ColumnNum)
}
