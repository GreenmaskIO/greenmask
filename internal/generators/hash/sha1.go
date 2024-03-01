package hash

import (
	"crypto/sha1"
	"fmt"
	"hash"
)

type Sha1 struct {
	hash.Hash
	salt []byte
	buf  []byte
}

func NewSha1(salt []byte) *Sha1 {
	return &Sha1{
		Hash: sha1.New(),
		buf:  make([]byte, sha1.Size),
		salt: salt,
	}
}

func (s *Sha1) Generate(data []byte) ([]byte, error) {
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
