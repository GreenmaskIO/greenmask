package transformers

import (
	"context"
	"fmt"
	"regexp"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var RegexpReplaceTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"RegexpReplace",
		"Replace string using regular expression",
		toolkit.TupleTransformation,
	),
	NewRegexpReplaceTransformer,
	toolkit.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("varchar", "text"),
		).SetRequired(true),
	toolkit.MustNewParameter(
		"regexp",
		"regular expression",
		new(string),
		nil,
	).SetRequired(true),
	toolkit.MustNewParameter(
		"replace",
		"replacement value",
		new(string),
		nil,
	).SetRequired(true),
)

type RegexpReplaceTransformerParams struct {
	Regexp   string  `mapstructure:"regexp" validate:"required"`
	Replace  string  `mapstructure:"replace" validate:"required"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RegexpReplaceTransformer struct {
	columnName string
	regexp     *regexp.Regexp
	replace    string
}

func NewRegexpReplaceTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, toolkit.ValidationWarnings, error) {
	var columnName, regexpStr, replace string
	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	p = parameters["regexp"]
	if err := p.Scan(&regexpStr); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "regexp" param: %w`, err)
	}

	p = parameters["replace"]
	if err := p.Scan(&replace); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "replace" param: %w`, err)
	}

	re, err := regexp.Compile(regexpStr)
	if err != nil {
		return nil, toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("parameterName", "regexp").
				SetMsg("cannot compile regular expression"),
		}, fmt.Errorf("cannot compile regular expression: %w", err)
	}

	return &RegexpReplaceTransformer{
		columnName: columnName,
		regexp:     re,
		replace:    replace,
	}, nil, nil

}

func (rrt *RegexpReplaceTransformer) Init(ctx context.Context) error {
	return nil
}

func (rrt *RegexpReplaceTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var original string
	if err := r.ScanAttribute(rrt.columnName, &original); err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}

	result := rrt.regexp.ReplaceAllString(original, rrt.replace)
	if err := r.SetAttribute(rrt.columnName, &result); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}
