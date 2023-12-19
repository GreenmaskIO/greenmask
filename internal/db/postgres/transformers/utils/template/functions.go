// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package template

import (
	"fmt"
	"maps"
	"math/rand"
	"reflect"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/ggwhite/go-masker"
	"github.com/go-faker/faker/v4"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
)

var NullValue NullType = "\\N"

type NullType string

const (
	MPassword   string = "password"
	MName       string = "name"
	MAddress    string = "addr"
	MEmail      string = "email"
	MMobile     string = "mobile"
	MTelephone  string = "tel"
	MID         string = "id"
	MCreditCard string = "credit_cart"
	MURL        string = "url"
	MDefault    string = "default"
)

var typeAliases = map[string]string{
	"bigint":                      "int8",
	"bit varying":                 "varbit",
	"boolean":                     "bool",
	"character":                   "char",
	"character varying":           "varchar",
	"double precision":            "float8",
	"integer":                     "int4",
	"int":                         "int4",
	"real":                        "float4",
	"numeric":                     "decimal",
	"smallint":                    "int2",
	"time without time zone":      "time",
	"time with time zone":         "timetz",
	"timestamp without time zone": "timestamp",
	"timestamp with time zone":    "timestamptz",
}

func FuncMap() template.FuncMap {

	randGen := rand.New(rand.NewSource(time.Now().UnixMicro()))
	typeMap := pgtype.NewMap()
	m := &masker.Masker{}
	faker.SetGenerateUniqueValues(false)

	Functions := template.FuncMap{
		"null":        getNullValue,
		"isNull":      valueIsNull,
		"isNotNull":   valueIsNotNull,
		"sqlCoalesce": sqlCoalesce,

		"jsonExists":     jsonExists,
		"mustJsonGet":    mustJsonGet,
		"mustJsonGetRaw": mustJsonGetRaw,
		"jsonGet":        jsonGet,
		"jsonGetRaw":     jsonGetRaw,
		"jsonSet":        mustJsonSet,      // TODO: Tests
		"jsonDelete":     mustJsonDelete,   // TODO: Tests
		"jsonSetRaw":     mustJsonSetRaw,   // TODO: Tests
		"jsonValidate":   mustJsonValidate, // TODO: Tests
		"jsonIsValid":    jsonIsValid,      // TODO: Tests
		"toJsonRawValue": toJsonRawValue,

		"isInt":    isInt,
		"isFloat":  isFloat,
		"isNil":    isNil,
		"isString": isString,
		"isMap":    isMap,
		"isSlice":  isSlice,
		"isBool":   isBool,

		"masking":      func(dataType string, v string) (string, error) { return masking(m, dataType, v) },
		"truncateDate": truncateDate,
		"noiseDatePgInterval": func(interval string, val time.Time) (time.Time, error) { // TODO: Implement interval validation, do not rely on driver
			return noiseDatePgInterval(typeMap, randGen, interval, val)
		},
		//"noiseDate": func(interval int64, val time.Time) time.Time { // TODO: tests
		//	return *(utils.NoiseDate(randGen, interval, &val))
		//},
		"noiseFloat": func(ratio any, precision int, value any) (float64, error) {
			return noiseFloat(randGen, precision, ratio, value)
		},
		"noiseInt": func(ratio any, value any) (int64, error) { return noiseInt(randGen, ratio, value) },

		"randomBool": func() bool { return utils.RandomBool(randGen) },
		"randomDate": func(min, max time.Time) (time.Time, error) { return randomDate(randGen, min, max) },
		"randomFloat": func(min, max any, precision ...any) (float64, error) {
			return randomFloat(randGen, min, max, precision...)
		},
		"randomInt": func(min, max any) (int64, error) { return randomInt(randGen, min, max) },
		"randomString": func(minLength, maxLength any, symbols ...string) (string, error) {
			return randomString(randGen, minLength, maxLength, symbols...)
		},

		"roundFloat": roundFloat,

		// Faker address
		"fakerRealAddress": faker.GetRealAddress,
		"fakerLatitude":    faker.Latitude,
		"fakerLongitude":   faker.Longitude,

		// Faker Datetime
		"fakerUnixTime":   faker.UnixTime,
		"fakerDate":       faker.Date,
		"fakerTimeString": faker.TimeString,
		"fakerMonthName":  faker.MonthName,
		"fakerYearString": faker.YearString,
		"fakerDayOfWeek":  faker.DayOfWeek,
		"fakerDayOfMonth": faker.DayOfMonth,
		"fakerTimestamp":  faker.Timestamp,
		"fakerCentury":    faker.Century,
		"fakerTimezone":   faker.Timezone,

		// Faker Internet
		"fakerEmail":      faker.Email,
		"fakerMacAddress": faker.MacAddress,
		"fakerDomainName": faker.DomainName,
		"fakerURL":        faker.URL,
		"fakerUsername":   faker.Username,
		"fakerIPv4":       faker.IPv4,
		"fakerIPv6":       faker.IPv6,
		"fakerPassword":   faker.Password,

		// Faker words and Sentences
		"fakerWord":      faker.Word,
		"fakerSentence":  faker.Sentence,
		"fakerParagraph": faker.Paragraph,

		// Faker Payment
		"fakerCCType":             faker.CCType,
		"fakerCCNumber":           faker.CCNumber,
		"fakerCurrency":           faker.Currency,
		"fakerAmountWithCurrency": faker.AmountWithCurrency,

		// Faker Person
		"fakerTitleMale":       faker.TitleMale,
		"fakerTitleFemale":     faker.TitleFemale,
		"fakerFirstName":       faker.FirstName,
		"fakerFirstNameMale":   faker.FirstNameMale,
		"fakerFirstNameFemale": faker.FirstNameFemale,
		"fakerFirstLastName":   faker.LastName,
		"fakerName":            faker.Name,

		// Faker Phone
		"fakerPhoneNumber":         faker.Phonenumber,
		"fakerTollFreePhoneNumber": faker.TollFreePhoneNumber,
		"fakerE164PhoneNumber":     faker.E164PhoneNumber,

		// Faker UUID
		"fakerUUIDHyphenated": faker.UUIDHyphenated,
		"fakerUUIDDigit":      faker.UUIDDigit,
	}

	tm := make(template.FuncMap)
	springFuncMap := sprig.FuncMap()
	maps.Copy(tm, springFuncMap)
	maps.Copy(tm, Functions)
	return tm
}

func sqlCoalesce(vv ...any) any {
	for _, v := range vv {
		if _, ok := v.(NullType); ok {
			continue
		}
		return v
	}
	return NullValue
}

func getNullValue() NullType {
	return NullValue
}

func valueIsNotNull(v any) bool {
	return !valueIsNull(v)
}

func valueIsNull(v any) bool {
	vv, ok := v.(NullType)
	if !ok {
		return false
	}
	return vv == NullValue
}

func jsonExists(path string, data string) bool {
	return gjson.Get(data, path).Exists()
}

func mustJsonGet(path string, data string) (interface{}, error) {
	res := gjson.Get(data, path)
	if !res.Exists() {
		return nil, fmt.Errorf("json path \"%s\" does not exist", path)
	}
	return res.Value(), nil
}

func mustJsonGetRaw(path string, data string) (string, error) {
	res := gjson.Get(data, path)
	if !res.Exists() {
		return "", fmt.Errorf("json path \"%s\" does not exist", path)
	}
	return res.Raw, nil
}

func jsonGet(path string, data string) interface{} {
	return gjson.Get(data, path).Value()
}

func jsonGetRaw(path string, data string) string {
	return gjson.Get(data, path).Raw
}

func mustJsonSet(path string, v any, data string) (string, error) {
	return sjson.Set(data, path, v)
}

func mustJsonDelete(path string, data string) (string, error) {
	return sjson.Delete(data, path)
}

func mustJsonSetRaw(path string, v string, data string) (string, error) {
	return sjson.SetRaw(data, path, v)
}

func isString(a any) bool {
	return reflect.TypeOf(a).Kind() == reflect.String
}

func isMap(a any) bool {
	return reflect.TypeOf(a).Kind() == reflect.Map
}

func isSlice(a any) bool {
	return reflect.TypeOf(a).Kind() == reflect.Slice
}

func isBool(a any) bool {
	return reflect.TypeOf(a).Kind() == reflect.Bool
}

func isInt(a any) bool {
	switch reflect.TypeOf(a).Kind() {
	case reflect.Int:
		return true
	case reflect.Int8:
		return true
	case reflect.Int16:
		return true
	case reflect.Int32:
		return true
	case reflect.Int64:
		return true
	case reflect.Uint:
		return true
	case reflect.Uint8:
		return true
	case reflect.Uint16:
		return true
	case reflect.Uint32:
		return true
	case reflect.Uint64:
		return true
	default:
		return false
	}
}

func mustJsonValidate(v string) (string, error) {
	if !gjson.Valid(v) {
		return "", fmt.Errorf("json is invalid: %s", v)
	}
	return v, nil
}

func jsonIsValid(v string) bool {
	return gjson.Valid(v)
}

func toJsonRawValue(v any) (string, error) {
	res, err := sjson.Set("", "a", v)
	if err != nil {
		return "", fmt.Errorf("error encoding %+v value into json type: %w", v, err)
	}
	return gjson.Get(res, "a").Raw, nil
}

func isFloat(a any) bool {
	switch reflect.TypeOf(a).Kind() {
	case reflect.Float32:
		return true
	case reflect.Float64:
		return true
	default:
		return false
	}
}

func isNil(a any) bool {
	return a == nil
}

func masking(m *masker.Masker, dataType string, v string) (string, error) {
	switch dataType {
	case MPassword:
		return m.Password(v), nil
	case MName:
		return m.Name(v), nil
	case MAddress:
		return m.Address(v), nil
	case MEmail:
		return m.Email(v), nil
	case MMobile:
		return m.Mobile(v), nil
	case MID:
		return m.ID(v), nil
	case MTelephone:
		return m.Telephone(v), nil
	case MCreditCard:
		return m.CreditCard(v), nil
	case MURL:
		return m.URL(v), nil
	case MDefault:
		return strings.Repeat("*", len(v)), nil
	default:
		return "", fmt.Errorf("wrong type masking \"%s\"", dataType)
	}
}

// TruncateDate - truncate date till the provided part of date
func truncateDate(part string, t time.Time) (time.Time, error) {
	res, err := utils.TruncateDate(&part, &t)
	if err != nil {
		return time.Time{}, err
	}
	return *res, nil
}

func noiseDatePgInterval(typeMap *pgtype.Map, randGen *rand.Rand, interval string, val time.Time) (time.Time, error) {
	t, _ := typeMap.TypeForName("interval")
	ratioInterval, err := t.Codec.DecodeValue(typeMap, t.OID, pgx.TextFormatCode, []byte(interval))
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing \"interval\" value \"%s\": %w", interval, err)
	}
	intervalValue, ok := ratioInterval.(pgtype.Interval)
	if !ok {
		return time.Time{}, fmt.Errorf(`cannot cast "ratio" param to interval value`)
	}
	ratio := (time.Duration(intervalValue.Days) * time.Hour * 24) +
		(time.Duration(intervalValue.Months) * 30 * time.Hour * 24) +
		(time.Duration(intervalValue.Microseconds) * time.Millisecond)

	return *(utils.NoiseDateV2(randGen, ratio, &val)), nil
}

func noiseFloat(randGen *rand.Rand, precision int, ratio any, value any) (float64, error) {
	if precision < 0 {
		return 0, fmt.Errorf("precision must be 0 or higher got %d", precision)
	}

	r, err := cast.ToFloat64E(ratio)
	if err != nil {
		return 0, fmt.Errorf("error casting ratio (%+v) to float64: %w", ratio, err)
	}

	v, err := cast.ToFloat64E(value)
	if err != nil {
		return 0, fmt.Errorf("error casting value (%+v) to float64: %w", value, err)
	}

	if r > 1 || r <= 0 {
		return 0, fmt.Errorf("ratio must be in interval (0, 1] got %f", ratio)
	}

	return utils.NoiseFloat(randGen, r, v, precision), nil
}

func noiseInt(randGen *rand.Rand, ratio any, value any) (int64, error) {
	r, err := cast.ToFloat64E(ratio)
	if err != nil {
		return 0, fmt.Errorf("error casting ratio (%+v) to float64: %w", ratio, err)
	}

	v, err := cast.ToInt64E(value)
	if err != nil {
		return 0, fmt.Errorf("error casting ratio (%+v) to int64: %w", value, err)
	}

	if r > 1 || r <= 0 {
		return 0, fmt.Errorf("ratio must be in interval (0, 1] got %f", ratio)
	}

	return utils.NoiseInt(randGen, r, v), nil
}

// randomDate - generate date randomly in the interval [min, max]
func randomDate(randGen *rand.Rand, min, max time.Time) (time.Time, error) {
	if min.After(max) {
		return time.Time{}, fmt.Errorf("min date (%s) must before the max date (%s)", min.String(), max.String())
	}
	return *(utils.RandomDate(randGen, &min, &max)), nil
}

// randomFloat - generate float randomly in the interval [min, max] with precision. By default precision is 4 digits
func randomFloat(randGen *rand.Rand, min, max any, precision ...any) (float64, error) {
	var err error
	var p = 4
	if len(precision) > 0 {
		p, err = cast.ToIntE(precision[0])
		if err != nil {
			return 0, fmt.Errorf("error casting \"precision\" (%+v) to int: %w", precision[0], err)
		}
		if p < 0 {
			return 0, fmt.Errorf("precision must be 0 or higher got %d", p)
		}
	}

	minFloat, err := cast.ToFloat64E(min)
	if err != nil {
		return 0, fmt.Errorf("error casting min (%+v) to float64: %w", min, err)
	}
	maxFloat, err := cast.ToFloat64E(max)
	if err != nil {
		return 0, fmt.Errorf("error casting max (%+v) to float64: %w", max, err)
	}
	if minFloat > maxFloat {
		return 0, fmt.Errorf("min value (%f) must be less than the max (%f)", minFloat, maxFloat)
	}
	return utils.RandomFloat(randGen, minFloat, maxFloat, p), nil
}

func randomInt(randGen *rand.Rand, min, max any) (int64, error) {
	minInt, err := cast.ToInt64E(min)
	if err != nil {
		return 0, fmt.Errorf("error casting min (%+v) to int64: %w", min, err)
	}

	maxInt, err := cast.ToInt64E(max)
	if err != nil {
		return 0, fmt.Errorf("error casting max (%+v) to int64: %w", max, err)
	}
	if minInt > maxInt {
		return 0, fmt.Errorf("min value (%d) must be less than the max (%d)", minInt, maxInt)
	}
	return utils.RandomInt(randGen, minInt, maxInt), nil
}

// randomString - generate random string in the provided min and max length using provided symbols. By default symbols
// are "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
func randomString(randGen *rand.Rand, minLength, maxLength any, symbols ...string) (string, error) {
	s := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	if len(symbols) > 0 {
		s = []rune(symbols[0])
	}

	minLengthInt, err := cast.ToInt64E(minLength)
	if err != nil {
		return "", fmt.Errorf("error casting minLength (%+v) to int64: %w", minLength, err)
	}

	maxLengthInt, err := cast.ToInt64E(maxLength)
	if err != nil {
		return "", fmt.Errorf("error casting maxLength (%+v) to int64: %w", maxLength, err)
	}
	if minLengthInt < 0 {
		return "", fmt.Errorf("minLength must be higher or equal 0 got %d", minLengthInt)
	}
	if maxLengthInt < 0 {
		return "", fmt.Errorf("maxLengthInt must be higher or equal 0 got %d", maxLengthInt)
	}
	if minLengthInt > maxLengthInt {
		return "", fmt.Errorf("minLength (%d) must be less or equal maxLengthInt (%d)", minLengthInt, maxLengthInt)
	}
	buf := make([]rune, maxLengthInt)
	return utils.RandomString(randGen, minLengthInt, maxLengthInt, s, buf), nil
}

func roundFloat(precision any, value any) (float64, error) {
	p, err := cast.ToIntE(precision)
	if err != nil {
		return 0, fmt.Errorf("error casting \"precision\" (%+v) to int: %w", precision, err)
	}
	if p < 0 {
		return 0, fmt.Errorf("precision must be 0 or higher got %d", p)
	}

	v, err := cast.ToFloat64E(value)
	if err != nil {
		return 0, fmt.Errorf("error casting value (%+v) to float64: %w", v, err)
	}
	return utils.Round(p, v), nil
}
