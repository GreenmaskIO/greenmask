package toolkit

import (
	"errors"
	"reflect"
)

func scanPointer(src, dest any) error {
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
			return errors.New("unable to set the value")
		}
		return errors.New("unexpected src type")
	}
	return errors.New("src must be pointer")
}
