package transformers

import (
	"context"
	"fmt"
	"slices"
	"strings"

	masker "github.com/ggwhite/go-masker"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
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

var MaskingTransformerDefinition = utils.NewDefinition(
	utils.NewTransformerProperties(
		"Masking",
		"Mask a value using one of masking type",
	),

	NewMaskingTransformer,

	toolkit2.MustNewParameter(
		"column",
		"column name",
	).SetIsColumn(toolkit2.NewColumnProperties().
		SetAffected(true).
		SetAllowedColumnTypes("text", "varchar"),
	).SetRequired(true),

	toolkit2.MustNewParameter(
		"type",
		"logical type of attribute (password name addr email mobile tel id credit url)",
	).SetRequired(true).
		SetRawValueValidator(maskerTypeValidator),
)

type maskingFunction func(val string) string

type MaskingTransformer struct {
	columnName      string
	masker          *masker.Masker
	maskingFunction maskingFunction
}

func NewMaskingTransformer(ctx context.Context, driver *toolkit2.Driver, parameters map[string]*toolkit2.Parameter) (utils.Transformer, toolkit2.ValidationWarnings, error) {

	var columnName string
	var dataType string
	var mf maskingFunction

	p := parameters["column"]
	if err := p.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("unable to scan column param: %w", err)
	}

	p = parameters["type"]
	if err := p.Scan(&dataType); err != nil {
		return nil, nil, fmt.Errorf("unable to scan type param: %w", err)
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
		return nil, nil, fmt.Errorf("wrong type: %s", dataType)
	}

	return &MaskingTransformer{
		columnName:      columnName,
		masker:          m,
		maskingFunction: mf,
	}, nil, nil
}

func (mt *MaskingTransformer) Init(ctx context.Context) error {
	return nil
}

func (mt *MaskingTransformer) Done(ctx context.Context) error {
	return nil
}

func (mt *MaskingTransformer) Transform(ctx context.Context, r *toolkit2.Record) (*toolkit2.Record, error) {
	var originalValue string
	isNull, err := r.ScanAttribute(mt.columnName, &originalValue)
	if err != nil {
		return nil, fmt.Errorf("unable to scan attribute value: %w", err)
	}
	if isNull {
		return r, nil
	}

	maskedValue := mt.maskingFunction(originalValue)
	if err := r.SetAttribute(mt.columnName, maskedValue); err != nil {
		return nil, fmt.Errorf("unable to set new value: %w", err)
	}
	return r, nil
}

func maskerTypeValidator(p *toolkit2.Parameter, v toolkit2.ParamsValue) (toolkit2.ValidationWarnings, error) {
	typeName := string(v)

	types := []string{MPassword, MName, MAddress, MEmail, MMobile, MTelephone, MID, MCreditCard, MURL}
	if !slices.Contains(types, typeName) {
		return toolkit2.ValidationWarnings{
			toolkit2.NewValidationWarning().
				SetSeverity(toolkit2.ErrorValidationSeverity).
				SetMsgf("unknown type %s: must be one of %s", typeName, strings.Join(types, ", ")),
		}, nil
	}
	return nil, nil
}

func init() {
	utils.DefaultTransformerRegistry.MustRegister(MaskingTransformerDefinition)
}
