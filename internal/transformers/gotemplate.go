package transformers

import (
	"bytes"
	"errors"
	"fmt"
	"text/template"

	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

// TODO: Test this transformer
var bufSize = 256

type GoTemplateTransformer struct {
	Column   pgDomains.ColumnMeta
	template *template.Template
	buf      *bytes.Buffer
}

func NewGoTemplateTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	templateStr, ok := params["template"]
	if !ok {
		return nil, errors.New("expected template key")
	}
	tmpl, err := template.New("static").Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("cannot parse template: %w", err)
	}
	b := make([]byte, 0, bufSize)
	return &GoTemplateTransformer{
		Column:   column,
		template: tmpl,
		buf:      bytes.NewBuffer(b),
	}, nil
}

func (gtt *GoTemplateTransformer) Transform(val string) (string, error) {
	gtt.buf.Reset()
	if err := gtt.template.Execute(gtt.buf, &val); err != nil {
		return "", fmt.Errorf("template reder error: %s", err)
	}
	return gtt.buf.String(), nil
}
