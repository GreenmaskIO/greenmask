package domains

import (
	"errors"
)

type Tuple struct {
	Table         *Table
	OriginalTuple []byte
	MaskedTuple   []byte
}

func (t *Tuple) MaskTuple() error {
	return errors.New("IMPLEMENT ME")
}

func (t *Tuple) GetMaskedTuple() []byte {
	return t.MaskedTuple
}
