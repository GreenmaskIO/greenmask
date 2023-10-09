package transformers

import (
	"context"
	"fmt"
	"regexp"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
)

var RegexpReplaceTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"RegexpReplace",
		"Replace string using regular expression",
	),

	NewRegexpReplaceTransformer,

	toolkit2.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit2.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("varchar", "text", "bpchar"),
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"regexp",
		"regular expression",
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"replace",
		"replacement value",
	).SetRequired(true),
)

type RegexpReplaceTransformer struct {
	columnName      string
	regexp          *regexp.Regexp
	replace         string
	affectedColumns map[int]string
}

func NewRegexpReplaceTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {
	var columnName, regexpStr, replace string
	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf(`unable to scan "column" param: %w`, err)
	}

	idx, _, ok := driver.GetColumnByName(columnName)
	if !ok {
		return nil, nil, fmt.Errorf("column with name %s is not found", columnName)
	}
	affectedColumns := make(map[int]string)
	affectedColumns[idx] = columnName

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
		return nil, toolkit2.ValidationWarnings{
			toolkit2.NewValidationWarning().
				SetSeverity(toolkit2.ErrorValidationSeverity).
				AddMeta("parameterName", "regexp").
				SetMsg("cannot compile regular expression"),
		}, fmt.Errorf("cannot compile regular expression: %w", err)
	}

	return &RegexpReplaceTransformer{
		columnName:      columnName,
		regexp:          re,
		replace:         replace,
		affectedColumns: affectedColumns,
	}, nil, nil

}

func (rrt *RegexpReplaceTransformer) GetAffectedColumns() map[int]string {
	return rrt.affectedColumns
}

func (rrt *RegexpReplaceTransformer) Init(ctx context.Context) error {
	return nil
}

func (rrt *RegexpReplaceTransformer) Done(ctx context.Context) error {
	return nil
}

func (rrt *RegexpReplaceTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
	var original string
	isNull, err := r.ScanAttribute(rrt.columnName, &original)
	if err != nil {
		return nil, fmt.Errorf("unable to scan value: %w", err)
	}
	if isNull {
		return r, nil
	}

	result := rrt.regexp.ReplaceAllString(original, rrt.replace)
	if err := r.SetAttribute(rrt.columnName, &result); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(RegexpReplaceTransformerDefinition)
}
