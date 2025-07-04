package utils

import (
	"errors"
	"reflect"
)

var (
	errUnableToSet       = errors.New("unable to set the value")
	errUnexpectedSrcType = errors.New("unexpected src type")
	errSrcMustBePointer  = errors.New("src must be pointer")
)

func ScanPointer(src, dest any) error {
	srcValue := reflect.ValueOf(src)
	destValue := reflect.ValueOf(dest)
	if srcValue.Kind() == destValue.Kind() {
		srcInd := reflect.Indirect(srcValue)
		destInd := reflect.Indirect(destValue)
		if srcInd.Kind() == destInd.Kind() {
			if srcInd.CanSet() {
				destInd.Set(srcInd)
				return nil
			}
			return errUnableToSet
		}
		return errUnexpectedSrcType
	}
	return errSrcMustBePointer
}
