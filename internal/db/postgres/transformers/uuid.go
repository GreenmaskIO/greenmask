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

const RandomUuidTransformerName = "RandomUuid"

var RandomUuidTransformerMeta = TransformerMeta{
	Description:    `Generate random UUID`,
	NewTransformer: NewRandomUuidTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetCastVar(uuid.New()).
		SetSupportedOids(
			pgtype.TextOID,
			pgtype.VarcharOID,
			pgtype.UUIDOID,
		).
		SetName(RandomUuidTransformerName),
}

type RandomUuidTransformerParams struct {
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RandomUuidTransformer struct {
	TransformerBase
	RandomUuidTransformerParams
	rand *rand.Rand
}

func NewRandomUuidTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	_, err := base.PgType.Codec.DecodeValue(base.TypeMap, uint32(base.Column.TypeOid), pgx.TextFormatCode, []byte("db9abb12-3e84-4873-915d-27c17a1fea22"))
	if err != nil {
		return nil, fmt.Errorf("cannot decode value: %w", err)
	}

	tParams := RandomUuidTransformerParams{
		Fraction: DefaultNullFraction,
	}
	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	return &RandomUuidTransformer{
		TransformerBase:             *base,
		RandomUuidTransformerParams: tParams,
		rand:                        rand.New(rand.NewSource(time.Now().UnixMicro())),
	}, nil
}

func (ut *RandomUuidTransformer) TransformAttr(val string) (string, error) {
	if ut.Nullable {
		if ut.rand.Float32() < ut.Fraction {
			return DefaultNullSeq, nil
		}
	}
	return uuid.New().String(), nil
}

func (ut *RandomUuidTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := getColumnValueFromCsvRecord(data, ut.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := ut.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return updateAttributeAndBuildRecord(record, transformedAttr, ut.ColumnNum)
}
