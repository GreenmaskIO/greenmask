package domains

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
)

type Table struct {
	Schema    string   `yaml:"schema"`
	Name      string   `yaml:"name"`
	Columns   []Column `yaml:"columns"`
	HasMasker bool
	Oid       int
	Owner     string
	DumpId    int32
}

func (t *Table) MakeTuple(data []byte) (*Tuple, error) {
	tuple := &Tuple{
		Table:         t,
		OriginalTuple: data,
	}
	log.Debug().Msgf("%+v\n", tuple)
	return nil, errors.New("IMPLEMENT ME")
}

func (t *Table) GetTocRecord() ([]byte, error) {
	if t.Oid == 0 {
		return nil, errors.New("oid cannot be 0")
	}
	if t.Schema == "" {
		return nil, errors.New("schema name cannot be empty")
	}

	return []byte(fmt.Sprintf("%d; %d TABLE DATA %s %s %s\n", t.DumpId, 0, t.Schema, t.Name, t.Owner)), nil
}
