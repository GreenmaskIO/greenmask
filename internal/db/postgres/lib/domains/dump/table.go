package dump

import (
	"errors"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	toolkit "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
)

type TocDumper interface {
	GetTocRecord() (*toc.Entry, error)
}

type Table struct {
	toolkit.Table
	Owner        string
	RelKind      rune
	RootPtSchema string
	RootPtName   string
	RootOid      toolkit.Oid
	Transformers []toolkit.Transformer
}

func (t *Table) GetTocRecord() (*toc.Entry, error) {
	return nil, errors.New("is not implemented")
}
