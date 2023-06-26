package transformers

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/crypto/scrypt"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

// TODO: Make length truncation

var HashTransformerSupportedOids = []int{
	pgtype.TextOID,
	pgtype.VarcharOID,
}

const saltLength = 32

var HashTransformerMeta = TransformerMeta{
	Description: `Replace with value passed through "value" parameter`,
	ParamsDescription: map[string]string{
		"salt": "secret salt that uses for applying hash function",
	},
	SupportedTypeOids: HashTransformerSupportedOids,
	NewTransformer:    NewHashTransformer,
}

type HashTransformerParams struct {
	Salt     string  `mapstructure:"salt"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type HashTransformer struct {
	TransformerBase
	HashTransformerParams
	Column pgDomains.ColumnMeta
	rand   *rand.Rand
	salt   []byte
}

func NewHashTransformer(
	column pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	useType string,
	params map[string]interface{},
) (domains.Transformer, error) {
	base, err := NewTransformerBase(column, typeMap, useType, HashTransformerSupportedOids, "")
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}

	tParams := HashTransformerParams{
		Fraction: DefaultNullFraction,
	}
	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	res := &HashTransformer{
		TransformerBase:       *base,
		HashTransformerParams: tParams,
		Column:                column,
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

func (rt *HashTransformer) Transform(val string) (string, error) {
	if val == DefaultNullSeq {
		return val, nil
	}
	if rt.Nullable {
		if rt.rand.Float32() < rt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	dk, err := scrypt.Key([]byte(val), rt.salt, 32768, 8, 1, 32)
	if err != nil {
		return "", fmt.Errorf("cannot generate hash by value %w", err)
	}
	return base64.StdEncoding.EncodeToString(dk), nil
}
