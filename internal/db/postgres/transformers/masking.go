package transformers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	masker "github.com/ggwhite/go-masker"
	"golang.org/x/exp/slices"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

const (
	MPassword   string = "password"
	MName       string = "name"
	MAddress    string = "addr"
	MEmail      string = "email"
	MMobile     string = "mobile"
	MTelephone  string = "tel"
	MID         string = "id"
	MCreditCard string = "credit"
	MURL        string = "url"
)

var MaskingTransformerDefinition = toolkit.NewDefinition(
	toolkit.MustNewTransformerProperties(
		"Masking",
		"Mask a value using one of masking type",
		toolkit.TupleTransformation,
	),
	NewMaskingTransformer,
	toolkit.MustNewParameter("column", "column name", new(string), nil).
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("text", "varchar"),
		).SetRequired(true),
	toolkit.MustNewParameter(
		"type",
		"logical type of attribute (password name addr email mobile tel id credit url)",
		new(string),
		nil,
	).SetRequired(true).
		SetValueValidator(maskerTypeValidator),
)

type maskingFunction func(val string) string

type MaskingTransformer struct {
	columnName      string
	masker          *masker.Masker
	maskingFunction maskingFunction
}

func NewMaskingTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (toolkit.Transformer, error) {

	var columnName string
	var dataType string
	var mf maskingFunction

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	p = parameters["type"]
	if err := p.Scan(&dataType); err != nil {
		return nil, fmt.Errorf("unable to scan type param: %w", err)
	}

	var m = &masker.Masker{}

	switch dataType {
	case MPassword:
		mf = m.Password
	case MName:
		mf = m.Name
	case MAddress:
		mf = m.Address
	case MEmail:
		mf = m.Email
	case MMobile:
		mf = m.Mobile
	case MID:
		mf = m.ID
	case MTelephone:
		mf = m.Telephone
	case MCreditCard:
		mf = m.CreditCard
	case MURL:
		mf = m.URL
	default:
		return nil, fmt.Errorf("wrong type: %s", dataType)
	}

	return &MaskingTransformer{
		columnName:      columnName,
		masker:          m,
		maskingFunction: mf,
	}, nil
}

func (mt *MaskingTransformer) Init(ctx context.Context) error {
	return nil
}

func (mt *MaskingTransformer) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	return nil, nil
}

func (mt *MaskingTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var originalValue string
	if err := r.ScanAttribute(mt.columnName, &originalValue); err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}

	if originalValue == toolkit.DefaultNullSeq {
		return r, nil
	}

	maskedValue := mt.maskingFunction(originalValue)
	if err := r.SetAttribute(mt.columnName, maskedValue); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func maskerTypeValidator(v any) (toolkit.ValidationWarnings, error) {
	typeStr, ok := v.(*string)
	if !ok {
		return nil, errors.New("expected string type")
	}
	types := []string{MPassword, MName, MAddress, MEmail, MMobile, MTelephone, MID, MCreditCard, MURL}
	if !slices.Contains(types, *typeStr) {
		return toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetLevel(toolkit.ErrorValidationSeverity).
				SetMsgf("unknown type %s: must be one of %s", *typeStr, strings.Join(types, ", ")),
		}, nil
	}
	return nil, nil
}

func init() {
	DefaultTransformerRegistry.MustRegister(MaskingTransformerDefinition)
}
