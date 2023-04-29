package faker

import (
	"errors"
	"fmt"
	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"strings"

	"github.com/jaswdr/faker"
	"golang.org/x/exp/slices"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type addressTransformerFunction func() string

type AddressTransformer struct {
	Column pgDomains.Column
	Type   string
	F      addressTransformerFunction
}

func NewAddressTransformer(column pgDomains.ColumnMeta, params map[string]string) (domains.Transformer, error) {
	types := []string{"CityPrefix", "SecondaryAddress", "State", "StateAbbr",
		"CitySuffix", "StreetSuffix", "BuildingNumber", "City", "StreetName",
		"StreetAddress", "PostCode", "Country", "CountryAbbr", "CountryCode",
		"Latitude", "Longitude"}
	credType, ok := params["type"]
	if !ok {
		return nil, errors.New("expected \"type\" argument")
	}
	if !slices.Contains(types, credType) {
		return nil, fmt.Errorf("unexpected transformer type %s expected one of %s", credType, strings.Join(types, ", "))
	}

	obj := CreditCardTransformer{
		Type: credType,
	}

	address := faker.Address{
		&faker.Faker{Generator: &RandomFakerGenerator{}},
	}

	switch credType {
	case "CityPrefix":
		obj.F = address.CityPrefix
	case "SecondaryAddress":
		obj.F = address.SecondaryAddress
	case "State":
		obj.F = address.State
	case "StateAbbr":
		obj.F = address.StateAbbr
	case "CitySuffix":
		obj.F = address.CitySuffix
	case "StreetSuffix":
		obj.F = address.StreetSuffix
	case "BuildingNumber":
		obj.F = address.BuildingNumber
	case "City":
		obj.F = address.City
	case "StreetName":
		obj.F = address.StreetName
	case "StreetAddress":
		obj.F = address.StreetAddress
	case "PostCode":
		obj.F = address.PostCode
	case "Country":
		obj.F = address.Country
	case "CountryAbbr":
		obj.F = address.CountryAbbr
	case "CountryCode":
		obj.F = address.CountryCode
	case "Latitude":
		obj.F = func() string {
			return address.Faker.Numerify("##.######")
		}
	case "Longitude":
		obj.F = func() string {
			return address.Faker.Numerify("##.######")
		}
	}

	return &obj, nil
}

func (at *AddressTransformer) Transform(originalValue string) (string, error) {
	return at.F(), nil
}
