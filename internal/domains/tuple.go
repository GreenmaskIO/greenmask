package domains

import (
	"errors"
)

type Tuple struct {
	Table         *Table
	OriginalTuple []byte
	MaskedTuple   []byte
	Attributes    map[string][]byte
}

func NewTuple(table *Table, data []byte) (*Tuple, error) {
	return &Tuple{
		Table:         table,
		OriginalTuple: data,
	}, nil
}

func (t *Tuple) MaskTuple() error {
	return errors.New("IMPLEMENT ME")
}

func (t *Tuple) GetMaskedTuple() []byte {
	return t.MaskedTuple
}
