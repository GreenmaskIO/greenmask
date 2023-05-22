package main

import (
	"fmt"
	"reflect"
)

func main() {
	var s interface{}
	s = CustomStruct{}

	PrintReflectionInfo(s)
	PrintReflectionInfo(&s)
}

type CustomStruct struct{}

func PrintReflectionInfo(v interface{}) {
	// expect CustomStruct if non pointer
	fmt.Println("Actual type is:", reflect.TypeOf(v))

	// expect struct if non pointer
	fmt.Println("Value type is:", reflect.ValueOf(v).Kind())

	if reflect.ValueOf(v).Kind() == reflect.Ptr {
		fmt.Println("Indirect type is:", reflect.Indirect(reflect.ValueOf(v)).Elem().Type()) // prints main.CustomStruct

		fmt.Println("Indirect value type is:", reflect.Indirect(reflect.ValueOf(v)).Elem().Kind()) // prints struct
	}

	fmt.Println("")
}
