package generators

import (
	"encoding/binary"
	"math/rand"
)

type HybridBytes struct {
	r           *rand.Rand
	g           Generator
	size        int
	randomIters int
	resBuf      []byte
	randomBuf   []byte
}

func NewHybridBytes(seed int64, requestedSize int, h Generator) *HybridBytes {
	genSize := h.Size()
	if genSize < 8 {
		panic("generator size must be at least 8 bytes")
	}

	var requiredRandomSize int
	if genSize < requestedSize {
		requiredRandomSize = requestedSize - genSize
	}

	randomIters := requiredRandomSize / 8
	if requiredRandomSize%8 > 0 {
		randomIters += 1
	}
	return &HybridBytes{
		r:           rand.New(rand.NewSource(seed)),
		g:           h,
		size:        requestedSize,
		randomIters: randomIters,
		resBuf:      make([]byte, requestedSize),
		randomBuf:   make([]byte, 8),
	}
}

func (hb *HybridBytes) Generate(data []byte) ([]byte, error) {
	hb.resBuf = hb.resBuf[:0]
	res, err := hb.g.Generate(data)
	if err != nil {
		return nil, err
	}
	hb.resBuf = append(hb.resBuf, res...)
	seed := int64(binary.LittleEndian.Uint64(res[len(res)-8:]))
	hb.r.Seed(seed)
	for i := 0; i < hb.randomIters; i++ {
		binary.LittleEndian.PutUint64(hb.randomBuf, hb.r.Uint64())
		hb.resBuf = append(hb.resBuf, hb.randomBuf...)
	}
	return hb.resBuf[:hb.size], nil
}
