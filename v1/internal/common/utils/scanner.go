package utils

import (
	"errors"
	"reflect"
)

var (
	errDestMustBePointer = errors.New("destination must be a pointer")
	errIncompatibleTypes = errors.New("incompatible types for assignment")
	errUnableToSet       = errors.New("destination value is not settable")
)

func ScanPointer(src, dest any) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.IsNil() {
		return errDestMustBePointer
	}

	// Dereference destination to get the actual value
	destElem := destVal.Elem()

	srcVal := reflect.ValueOf(src)
	if !srcVal.IsValid() {
		// If src is nil, set dest to zero value
		destElem.Set(reflect.Zero(destElem.Type()))
		return nil
	}

	// If src is a pointer, dereference it
	if srcVal.Kind() == reflect.Ptr {
		if srcVal.IsNil() {
			destElem.Set(reflect.Zero(destElem.Type()))
			return nil
		}
		srcVal = srcVal.Elem()
	}

	// Convert or assign if possible
	if !srcVal.Type().AssignableTo(destElem.Type()) {
		return errIncompatibleTypes
	}
	if !destElem.CanSet() {
		return errUnableToSet
	}

	destElem.Set(srcVal)
	return nil
}
