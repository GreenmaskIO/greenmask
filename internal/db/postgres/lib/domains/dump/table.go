package dump

import toolkit "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"

type Table struct {
	toolkit.Table
	Owner        string
	RelKind      rune
	RootPtSchema string
	RootPtName   string
	RootOid      toolkit.Oid
	Transformers []toolkit.Transformer
}
