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

// TODO: Consider about strict type usage and provide useful function to work with
var bufSize = 256

type GoTemplateTransformer struct {
	Column     pgDomains.ColumnMeta
	Template   *template.Template
	Buf        *bytes.Buffer
	PgType     *pgtype.Type
	EncodePlan pgtype.EncodePlan
}

var GoTemplateTransformerMata = TransformerMeta{
	Description: "Apply golang template value",
	ParamsDescription: map[string]string{
		"template": "go template string",
	},
	SupportedTypeOids: []int{
		pgtype.TextOID,
		pgtype.VarcharOID,
	},
	NewTransformer: NewGoTemplateTransformer,
}

func NewGoTemplateTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	var cast string
	templateStr, ok := params["template"]
	if !ok {
		return nil, errors.New("expected Template key")
	}
	tmpl, err := template.New("t").Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("cannot parse Template: %w", err)
	}
	buf := bytes.NewBuffer(make([]byte, 0, bufSize))

	t, plan, err := GetPgCodeAndEncodingPlan(typeMap, column.TypeOid, cast)
	if err != nil {
		return nil, err
	}

	return &GoTemplateTransformer{
		Column:     column,
		Template:   tmpl,
		Buf:        buf,
		PgType:     t,
		EncodePlan: plan,
	}, nil
}

func (gtt *GoTemplateTransformer) Transform(val string) (string, error) {
	gtt.Buf.Reset()
	if err := gtt.Template.Execute(gtt.Buf, val); err != nil {
		return "", fmt.Errorf("Template reder error: %s", err)
	}
	res, err := gtt.EncodePlan.Encode(gtt.Buf.String(), nil)
	if err != nil {
		return "", fmt.Errorf("cannot convert result of template into pg type: %w", err)
	}
	return string(res), nil
}
