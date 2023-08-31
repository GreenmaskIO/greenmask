package dump

import (
	"errors"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/toc"
)

type LargeObject struct {
	Name string
}

func (lo *LargeObject) GetTocRecord() (*toc.Entry, error) {
	return nil, errors.New("is not implemented")
}
