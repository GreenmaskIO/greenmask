package dump

import (
	"errors"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/toc"
)

type Sequence struct {
	Schema string
	Name   string
	Oid    string
}

func (s *Sequence) GetTocRecord() (*toc.Entry, error) {
	return nil, errors.New("is not implemented")
}
