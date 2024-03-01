package random

import (
	"math/rand"
)

type Int64 struct {
	r    *rand.Rand
	size int
}

func NewInt64(seed int64) *Int64 {
	return &Int64{
		r:    rand.New(rand.NewSource(seed)),
		size: 4,
	}
}

func (i *Int64) Generate(data []byte) ([]byte, error) {
	value := i.r.Int63()
	res := make([]byte, i.size)
	for shift := 0; shift < i.size; shift++ {
		res[shift] = byte((value << (8 * shift)) & 0xFF)
	}
	return res, nil
}

func (i *Int64) Size() int {
	return i.size
}
