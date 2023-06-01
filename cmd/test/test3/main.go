package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"
)

func main() {

	c, err := pgx.Connect(context.Background(), "user=vvoitenko dbname=demo host=/tmp")
	if err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}
	typeMap := c.TypeMap()

	//t, ok := typeMap.TypeForName("date")
	t, ok := typeMap.TypeForOID(pgtype.DateOID)
	if !ok {
		log.Fatal().Msg("unknown type")
	}
	val, err := t.Codec.DecodeDatabaseSQLValue(typeMap, t.OID, pgx.TextFormatCode, []byte("2017-09-14"))

	if err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}
	switch v := val.(type) {
	case time.Time:
		log.Print(v)
		v = v.AddDate(0, 0, 2)
		log.Print(v)
		plan := typeMap.PlanEncode(t.OID, pgx.TextFormatCode, v)
		if plan == nil {
			log.Fatal().Msg("unable to determine plan for type")
		}
		buf, err := plan.Encode(v, nil)
		if err != nil {
			log.Fatal().Err(err).Msg("fatal")
		}
		log.Print(string(buf))

	default:
		log.Debug().Msg("unknown type")
	}

}
