package transformers

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

// TODO: Test this transformer

type RandomDateTransformer struct {
	Column     pgDomains.ColumnMeta
	PgType     *pgtype.Type
	EncodePlan pgtype.EncodePlan
	startDate  time.Time
	endDate    time.Time
	delta      int64
}

func NewRandomDateTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	var res = &RandomDateTransformer{}
	if typeMap == nil {
		return nil, errors.New("typeMap cannot be nil")
	}
	start, ok := params["start"]
	if !ok {
		return nil, errors.New("expected start key")
	}
	if start == "" {
		return nil, errors.New("start key cannot be empty string")
	}
	end, ok := params["end"]
	if !ok {
		return nil, errors.New("expected end key")
	}
	if end == "" {
		return nil, errors.New("end key cannot be empty string")
	}

	t, ok := typeMap.TypeForOID(column.TypeOid)
	if !ok {
		return nil, fmt.Errorf("cannot match type with pg type %d", column.TypeOid)
	}
	res.PgType = t

	plan := typeMap.PlanEncode(t.OID, pgx.TextFormatCode, res.startDate)
	if plan == nil {
		return nil, fmt.Errorf("cannot find encoding plan for oid %d", t.OID)
	}
	res.EncodePlan = plan

	val, err := t.Codec.DecodeValue(typeMap, t.OID, pgx.TextFormatCode, []byte(start))
	if err != nil {
		return nil, fmt.Errorf("cannot decode start value: %w", err)
	}
	switch v := val.(type) {
	case time.Time:
		res.startDate = v
	default:
		return nil, errors.New("cannot cast type of start key")
	}

	val, err = t.Codec.DecodeValue(typeMap, t.OID, pgx.TextFormatCode, []byte(end))
	if err != nil {
		return nil, fmt.Errorf("cannot decode end value: %w", err)
	}
	switch v := val.(type) {
	case time.Time:
		res.endDate = v
	default:
		return nil, fmt.Errorf("cannot cast type of end key: unexpected type %+v", v)
	}
	res.delta = res.endDate.UnixMicro() - res.startDate.UnixMicro()
	return res, nil
}

func (gtt *RandomDateTransformer) Transform(val string) (string, error) {
	resTime := time.UnixMicro(rand.Int63n(gtt.delta) + gtt.startDate.UnixMicro())
	res, err := gtt.EncodePlan.Encode(resTime, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}
