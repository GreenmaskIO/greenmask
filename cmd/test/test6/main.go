package main

import (
	"encoding/json"
	"errors"
	"reflect"

	"github.com/rs/zerolog/log"
)

func main() {
	raw := []byte("9223372036854775807")
	params := 0
	if err := json.Unmarshal(raw, &params); err != nil {
		log.Fatal().Err(err).Msg("")
	}
	// bool, for JSON booleans
	// float64, for JSON numbers
	// string, for JSON strings
	// []interface{}, for JSON arrays
	// map[string]interface{}, for JSON objects
	// nil for JSON null
	println(params)
}

func scan(originalValue any) error {
	defaultValue := 1
	var defaultValueInterface any = &defaultValue
	defaultReflectValue := reflect.ValueOf(defaultValueInterface)
	originalReflectValue := reflect.ValueOf(originalValue)
	if originalReflectValue.Kind() == defaultReflectValue.Kind() {
		indOr := reflect.Indirect(originalReflectValue)
		indDef := reflect.Indirect(defaultReflectValue)
		if indOr.Kind() == indDef.Kind() {
			if indOr.CanSet() {
				indOr.Set(indDef)
				return nil
			}
			return errors.New("unable to set the value")
		}
		//fmt.Println("Indirect type is:", ind) // prints main.CustomStruct
		//fmt.Println("Indirect value type is:", reflect.Indirect(reflect.ValueOf(originalReflectValue)).Elem().Kind()) // prints struct

	}
	return errors.New("originalValue must be pointer")
}
