// Copyright 2025 Greenmask
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
