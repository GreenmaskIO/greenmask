package hash

import (
	"hash"

	"github.com/spaolacci/murmur3"
)

type MurmurHash struct {
	hash.Hash32
}

func NewMurmurHash() *MurmurHash {
	return &MurmurHash{
		Hash32: murmur3.New32(),
	}
}

func (mh *MurmurHash) Generate(data []byte) ([]byte, error) {
	return mh.Sum(data), nil
}
