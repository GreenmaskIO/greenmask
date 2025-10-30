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
	"math/rand"
)

type Int64Random struct {
	r    *rand.Rand
	size int
}

func NewInt64Random(seed int64) (*Int64Random, error) {
	return &Int64Random{
		r:    rand.New(rand.NewSource(seed)),
		size: 8,
	}, nil
}

func (i *Int64Random) Generate(data []byte) ([]byte, error) {
	res := make([]byte, i.size)
	binary.LittleEndian.PutUint64(res, i.r.Uint64())
	return res, nil
}

func (i *Int64Random) Size() int {
	return i.size
}
