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
	"fmt"
	"hash"

	"github.com/spaolacci/murmur3"
)

const (
	MurMurHash32Size  = 4
	MurMurHash64Size  = 8
	MurMurHash128Size = 16
)

type MurmurHash struct {
	hash.Hash
	size int
}

func NewMurmurHash(seed uint32, size int) *MurmurHash {
	var h hash.Hash
	switch size {
	case MurMurHash32Size:
		h = murmur3.New32WithSeed(seed)
	case MurMurHash64Size:
		h = murmur3.New64WithSeed(seed)
	case MurMurHash128Size:
		h = murmur3.New128WithSeed(seed)
	default:
		panic(fmt.Sprintf("unknown size for hash %d", size))
	}
	return &MurmurHash{
		Hash: h,
		size: size,
	}
}

func (mh *MurmurHash) Size() int {
	return mh.size
}

func (mh *MurmurHash) Generate(data []byte) ([]byte, error) {
	return mh.Sum(data), nil
}
