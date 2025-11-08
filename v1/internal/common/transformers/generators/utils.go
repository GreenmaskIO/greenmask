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

package generators

import (
	"encoding/binary"
	"fmt"
)

func BuildBytesFromInt64(value int64) []byte {
	res := make([]byte, 8)
	binary.LittleEndian.PutUint64(res, uint64(value))
	return res
}

// BuildInt64FromBytes - decode bytes array to int64 representation. In case there is less
func BuildInt64FromBytes(data []byte) (res int64) {
	intBytes := data
	if len(data) != 8 {
		intBytes = make([]byte, 8)
		copy(intBytes, data[:8])
	}

	return int64(binary.LittleEndian.Uint64(intBytes))
}

func BuildBytesFromUint64(value uint64) []byte {
	res := make([]byte, 8)
	binary.LittleEndian.PutUint64(res, value)
	return res
}

func BuildUint64FromBytes(data []byte) (res uint64) {
	intBytes := data
	if len(data) != 8 {
		intBytes = make([]byte, 8)
		copy(intBytes, data[:8])
	}

	return binary.LittleEndian.Uint64(intBytes)
}

func GetHashBytesGen(salt []byte, size int) (Generator, error) {
	hashFunctionName, hashSize, err := GetHashFunctionNameBySize(size)
	if err != nil {
		return nil, fmt.Errorf("unable to determine hash function for deterministic transformer: %w", err)
	}
	g, err := NewHash(salt, hashFunctionName)
	if err != nil {
		return nil, fmt.Errorf("cannot create hash function backend: %w", err)
	}
	if size < hashSize {
		g = NewHashReducer(g, size)
	}

	return g, nil
}

func GetHashFunctionNameBySize(size int) (string, int, error) {
	if size <= 28 {
		return Sha3224, 28, nil
	} else if size <= 32 {
		return Sha3256, 32, nil
	} else if size <= 48 {
		return Sha3384, 48, nil
	} else if size <= 64 {
		return Sha3512, 64, nil
	}
	return "", 0, fmt.Errorf("unable to find suitable hash function for requested %d size", size)
}
