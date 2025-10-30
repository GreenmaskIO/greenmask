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
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"

	"golang.org/x/crypto/sha3"
)

const (
	Sha1Name   = "sha1"
	Sha256Name = "sha256"
	Sha512Name = "sha512"
	Sha3224    = "sha3-224"
	Sha3256    = "sha3-256"
	Sha3384    = "sha3-384"
	Sha3512    = "sha3-512"
)

type Hash struct {
	hash.Hash
	salt []byte
	buf  []byte
}

func NewHash(salt []byte, funcName string) (Generator, error) {

	var h hash.Hash

	switch funcName {
	case Sha1Name:
		h = sha1.New()
	case Sha256Name:
		h = sha256.New()
	case Sha512Name:
		h = sha512.New()
	case Sha3224:
		h = sha3.New224()
	case Sha3256:
		h = sha3.New256()
	case Sha3384:
		h = sha3.New384()
	case Sha3512:
		h = sha3.New512()
	default:
		return nil, fmt.Errorf("unknow hash function name \"%s\"", funcName)
	}

	size := h.Size()

	return &Hash{
		Hash: h,
		buf:  make([]byte, size),
		salt: salt,
	}, nil
}

func (s *Hash) Generate(data []byte) ([]byte, error) {
	defer s.Reset()
	_, err := s.Write(s.salt)
	if err != nil {
		return nil, fmt.Errorf("unable to write salt into writer: %w", err)
	}
	_, err = s.Write(data)
	if err != nil {
		return nil, fmt.Errorf("unable to write data into writer: %w", err)
	}

	s.buf = s.buf[:0]
	return s.Sum(s.buf), nil
}
