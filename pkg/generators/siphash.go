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
