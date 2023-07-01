package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const (
	AnyOid              = -1
	DefaultNullFraction = 0.3
)

var UuidTransformerSupportedOids = []int{
	pgtype.TextOID,
	pgtype.VarcharOID,
	pgtype.UUIDOID,
}

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
	Description:       `Generate random UUID`,
	SupportedTypeOids: UuidTransformerSupportedOids,
	NewTransformer:    NewUuidTransformer,
	Settings: NewTransformerSettings().
		SetNullable(),
}

func NewUuidTransformer(
	table *pgDomains.TableMeta,
	column *pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	params map[string]interface{},
) (domains.Transformer, error) {
	base, err := NewTransformerBase(table, column, UuidTransformerMeta.Settings, params, typeMap, UuidTransformerSupportedOids, uuid.New())
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}

	_, err = base.PgType.Codec.DecodeValue(typeMap, uint32(column.TypeOid), pgx.TextFormatCode, []byte("db9abb12-3e84-4873-915d-27c17a1fea22"))
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
