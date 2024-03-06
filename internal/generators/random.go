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

type BytesRandom struct {
	r     *rand.Rand
	size  int
	iters int
}

func NewBytesRandom(seed int64, size int) *BytesRandom {
	iters := size / 8
	if size%8 > 0 {
		iters += 1
	}
	return &BytesRandom{
		r:     rand.New(rand.NewSource(seed)),
		size:  size,
		iters: iters,
	}
}

func (br *BytesRandom) Generate(data []byte) ([]byte, error) {
	res := make([]byte, 0, br.size)
	buf := make([]byte, 8)
	for i := 0; i < br.iters; i++ {
		binary.LittleEndian.PutUint64(buf, br.r.Uint64())
		res = append(res, buf...)
	}
	return res[:br.size], nil
}

func (br *BytesRandom) Size() int {
	return br.size
}
