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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScanPointer(t *testing.T) {
	t.Run("assign int to *int", func(t *testing.T) {
		var dst int
		err := ScanPointer(42, &dst)
		assert.NoError(t, err)
		assert.Equal(t, 42, dst)
	})

	t.Run("assign *int to *int", func(t *testing.T) {
		src := 99
		var dst int
		err := ScanPointer(&src, &dst)
		assert.NoError(t, err)
		assert.Equal(t, 99, dst)
	})

	t.Run("assign string to *string", func(t *testing.T) {
		var dst string
		err := ScanPointer("hello", &dst)
		assert.NoError(t, err)
		assert.Equal(t, "hello", dst)
	})

	t.Run("assign *string to *string", func(t *testing.T) {
		src := "world"
		var dst string
		err := ScanPointer(&src, &dst)
		assert.NoError(t, err)
		assert.Equal(t, "world", dst)
	})

	t.Run("assign nil pointer to *int", func(t *testing.T) {
		var src *int
		var dst int
		err := ScanPointer(src, &dst)
		assert.NoError(t, err)
		assert.Equal(t, 0, dst)
	})

	t.Run("assign incompatible types", func(t *testing.T) {
		var dst int
		err := ScanPointer("not-an-int", &dst)
		assert.ErrorIs(t, err, errIncompatibleTypes)
	})

	t.Run("dest is not a pointer", func(t *testing.T) {
		dst := 123
		err := ScanPointer(456, dst)
		assert.ErrorIs(t, err, errDestMustBePointer)
	})

	t.Run("src is nil interface", func(t *testing.T) {
		var dst int
		var src any = nil
		err := ScanPointer(src, &dst)
		assert.NoError(t, err)
		assert.Equal(t, 0, dst)
	})
}
