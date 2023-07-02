package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type UuidTransformerParams struct {
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type UuidTransformer struct {
	TransformerBase
	UuidTransformerParams
	rand *rand.Rand
}

var UuidTransformerMeta = TransformerMeta{
	Description:    `Generate random UUID`,
	NewTransformer: NewUuidTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetCastVar(uuid.New()).
		SetSupportedOids(
			pgtype.TextOID,
			pgtype.VarcharOID,
			pgtype.UUIDOID,
		),
}

func NewUuidTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	_, err := base.PgType.Codec.DecodeValue(base.TypeMap, uint32(base.Column.TypeOid), pgx.TextFormatCode, []byte("db9abb12-3e84-4873-915d-27c17a1fea22"))
	if err != nil {
		return nil, fmt.Errorf("cannot decode value: %w", err)
	}

	tParams := UuidTransformerParams{
		Fraction: DefaultNullFraction,
	}
	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	return &UuidTransformer{
		TransformerBase:       *base,
		UuidTransformerParams: tParams,
		rand:                  rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil
}

func (rt *UuidTransformer) Transform(val string) (string, error) {
	if rt.Nullable {
		if rt.rand.Float32() < rt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	return uuid.New().String(), nil
}
