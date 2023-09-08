package utils

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
)

func GetPgTypeAndEncodingPlan(typeMap *pgtype.Map, typeOid toc.Oid, castVal any) (*pgtype.Type, pgtype.EncodePlan, error) {
	t, ok := typeMap.TypeForOID(uint32(typeOid))
	if !ok {
		return nil, nil, fmt.Errorf("cannot match pgtype %d", typeOid)
	}

	plan := typeMap.PlanEncode(t.OID, pgx.TextFormatCode, castVal)
	if plan == nil {
		return nil, nil, fmt.Errorf("cannot find encoding plan for oid %d", t.OID)
	}
	return t, plan, nil
}

func scan(src any, dest interface{}) error {
	if reflect.ValueOf(dest).Kind() == reflect.Ptr {
		destType := reflect.Indirect(reflect.ValueOf(dest)).Type()
		valType := reflect.TypeOf(src)
		if destType != valType &&
			(!strings.Contains(destType.Name(), "int") && !strings.Contains(valType.Name(), "int")) &&
			(!strings.Contains(destType.Name(), "float") && !strings.Contains(valType.Name(), "float")) {
			return fmt.Errorf("unpexpected types")
		}
	} else {
		return fmt.Errorf("expected pointer")
	}

	switch destTyped := dest.(type) {
	case *time.Time:
		valTyped, ok := src.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time value")
		}
		reflect.ValueOf(destTyped).Elem().Set(reflect.ValueOf(&valTyped).Elem())
	case *int64:
		var castVar int64
		switch v := src.(type) {
		case int16:
			castVar = int64(v)
		case int32:
			castVar = int64(v)
		case int64:
			castVar = v
		default:
			return fmt.Errorf("expected int64 value")
		}
		reflect.ValueOf(destTyped).Elem().Set(reflect.ValueOf(&castVar).Elem())
	case *float64:
		var castVar float64
		switch v := src.(type) {
		case float32:
			castVar = float64(v)
		case float64:
			castVar = v
		default:
			return fmt.Errorf("expected float64 value")
		}
		reflect.ValueOf(destTyped).Elem().Set(reflect.ValueOf(&castVar).Elem())
	default:
		return fmt.Errorf("unsopported type")
	}

	return nil
}

func Scan(src string, dest interface{}, oid uint32, typeMap *pgtype.Map, pgType *pgtype.Type) error {
	// TODO: You should get rid of str string use instead src []byte
	val, err := pgType.Codec.DecodeValue(typeMap, oid, pgx.TextFormatCode, []byte(src))
	if err != nil {
		return fmt.Errorf("cannot decode min value: %w", err)
	}

	return scan(val, dest)
}
