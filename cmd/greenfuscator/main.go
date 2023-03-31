package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"log"

	"github.com/wwoytenko/greenfuscator/internal/domains"
	"github.com/wwoytenko/greenfuscator/internal/masker/simple"
)

var metricsTable = domains.Table{
	Schema: "public",
	Name:   "metrics",
	Columns: map[string]domains.Column{
		"id": {
			Name:   "id",
			Type:   "TEXT",
			Masker: &simple.DummyMasker{},
		},
		"type": {
			Name:   "type",
			Type:   "TEXT",
			Masker: &simple.DummyMasker{},
		},
		"value": {
			Name:   "value",
			Type:   "double precision",
			Masker: &simple.DummyMasker{},
		},
		"delta": {
			Name:   "delta",
			Type:   "double precision",
			Masker: &simple.DummyMasker{},
		},
		"created_at": {
			Name:   "created_at",
			Type:   "timestamp without time zone",
			Masker: &simple.DummyMasker{},
		},
	},
}

func main() {
	con, err := pgx.Connect(context.Background(), "host=localhost user=postgres database=test")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(con)

	if err := con.Ping(context.Background()); err != nil {
		log.Fatal(err)
	}

	if err = DumpTable(con, &metricsTable); err != nil {
		log.Fatal(err)
	}

}

func maskTuple(tuple *domains.Tuple) (*domains.Tuple, error) {
	return tuple, nil
}

func DumpTable(con *pgx.Conn, table *domains.Table) error {

	var copyOutResponse *pgproto3.CopyOutResponse

	frontend := con.PgConn().Frontend()
	frontend.Send(&pgproto3.Query{
		String: fmt.Sprintf("COPY \"%s\".\"%s\" TO STDOUT", table.Schema, table.Name),
	})
	if err := frontend.Flush(); err != nil {
		return err
	}

	for {
		msg, err := frontend.Receive()
		if err != nil {
			// TODO: You must send asynchronous message that you have stopped in error
			return fmt.Errorf("unable perform copy query: %w", err)
		}
		switch v := msg.(type) {
		case *pgproto3.CopyOutResponse:
			copyOutResponse = v
			log.Println(copyOutResponse)
		case *pgproto3.CopyData:
			tuple, err := maskTuple(&domains.Tuple{Table: *table, Tuple: v.Data})
			if err != nil {
				return fmt.Errorf("cannot mask tuple: %w", err)
			}
			if err := WriteTupleToFile(tuple); err != nil {
				return fmt.Errorf("cannot dump data: %w", err)
			}
		case *pgproto3.CopyDone:
		case *pgproto3.CommandComplete:
		case *pgproto3.ReadyForQuery:
			return nil
		default:
			return fmt.Errorf("unknown backup message %+v", v)
		}
	}
}

func WriteTupleToFile(tuple *domains.Tuple) error {
	log.Print(string(tuple.Tuple))
	return nil
}
