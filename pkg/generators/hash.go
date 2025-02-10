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
