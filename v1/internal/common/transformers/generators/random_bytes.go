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

type RandomBytes struct {
	r     *rand.Rand
	size  int
	iters int
}

func NewRandomBytes(seed int64, size int) *RandomBytes {
	iters := size / 8
	if size%8 > 0 {
		iters += 1
	}
	return &RandomBytes{
		r:     rand.New(rand.NewSource(seed)),
		size:  size,
		iters: iters,
	}
}

func (br *RandomBytes) Generate(data []byte) ([]byte, error) {
	res := make([]byte, 0, br.size)
	buf := make([]byte, 8)
	for i := 0; i < br.iters; i++ {
		binary.LittleEndian.PutUint64(buf, br.r.Uint64())
		res = append(res, buf...)
	}
	return res[:br.size], nil
}

func (br *RandomBytes) Size() int {
	return br.size
}
