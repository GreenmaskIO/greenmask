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
