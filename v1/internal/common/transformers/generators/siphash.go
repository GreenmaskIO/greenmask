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

	"github.com/dchest/siphash"
	"golang.org/x/crypto/sha3"
)

type SipHash struct {
	hash.Hash
	salt []byte
	buf  []byte
	size int
}

func NewSipHash(salt []byte) (Generator, error) {

	salt = sha3.New224().Sum(salt)[:16]

	h := siphash.New(salt)

	return &SipHash{
		Hash: h,
		buf:  make([]byte, 8),
		salt: salt,
		size: 8,
	}, nil
}

func (s *SipHash) Generate(data []byte) ([]byte, error) {
	defer s.Reset()

	if _, err := s.Write(data); err != nil {
		return nil, fmt.Errorf("unable to write data into writer: %w", err)
	}

	s.buf = s.buf[:0]
	return s.Sum(s.buf), nil
}
