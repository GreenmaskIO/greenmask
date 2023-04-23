package faker

import (
	"errors"
	"fmt"
	"github.com/jaswdr/faker"
	"github.com/wwoytenko/greenfuscator/internal/domains"
	"golang.org/x/exp/slices"
	"strings"
)

type creditCardTransformerFunc func() string

type CreditCardTransformer struct {
	Column  domains.ColumnMeta
	Type    string
	payment faker.Payment
	F       creditCardTransformerFunc
}

func NewCreditCardTransformer(column domains.ColumnMeta, params map[string]string) (domains.Transformer, error) {
	types := []string{"Number", "VendorName", "ExpirationDateString"}
	credType, ok := params["type"]
	if !ok {
		return nil, errors.New("expected \"type\" argument")
	}
	if !slices.Contains(types, credType) {
		return nil, fmt.Errorf("unexpected transformer type %s expected one of %s", credType, strings.Join(types, ", "))
	}

	payment := faker.Payment{
		Faker: &faker.Faker{
			Generator: &RandomFakerGenerator{},
		},
	}

	obj := CreditCardTransformer{
		Type:    credType,
		payment: payment,
	}

	switch credType {
	case "Number":
		obj.F = payment.CreditCardNumber
	case "VendorName":
		obj.F = payment.CreditCardType
	case "ExpirationDateString":
		obj.F = payment.CreditCardExpirationDateString
	}

	return &obj, nil
}

func (cct *CreditCardTransformer) Transform(originalValue string) (string, error) {
	return cct.F(), nil
}
