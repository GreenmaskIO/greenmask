package transformers

import (
	"encoding/binary"
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/generators"
)

const stringTransformerMaxHashLength = 64
const stringLengthByteSize = 4

type RandomStringTransformer struct {
	byteLength                 int
	generator                  generators.Generator
	characters                 []rune
	minLength                  int
	maxLength                  int
	requiredBytesPerCharLength int
	offsetFromMinLen           int
	buf                        []rune
}

func NewRandomStringTransformer(chars []rune, minLength, maxLength int) (*RandomStringTransformer, error) {

	if minLength > maxLength {
		return nil, fmt.Errorf("minLength (%d) is greater than maxLength (%d)", minLength, maxLength)
	}

	requiredBytesPerChar := 1
	if len(chars) > 256 {
		requiredBytesPerChar = 2
	}

	// Since the implementation is written for hash and random we do the next steps
	// 1. Determine the max length of the hash function (this length will be requested for random as well)
	// 2. If the max length is greater than the max length of the string, we shift the char position by the one with
	//    modulo of the length of the chars
	// 3. The max byte sequence is 64 bytes
	_, byteLength, err := generators.GetHashFunctionNameBySize((maxLength + requiredBytesPerChar) % stringTransformerMaxHashLength)
	if err != nil {
		return nil, err
	}

	return &RandomStringTransformer{
		characters:                 chars,
		minLength:                  minLength,
		maxLength:                  maxLength,
		byteLength:                 byteLength,
		requiredBytesPerCharLength: requiredBytesPerChar,
		buf:                        make([]rune, maxLength),
		offsetFromMinLen:           maxLength - minLength + 1,
	}, nil
}

func (st *RandomStringTransformer) Transform(data []byte) []rune {
	clear(st.buf)
	resBytes, _ := st.generator.Generate(data)

	stringLength := int(binary.LittleEndian.Uint32(resBytes[:stringLengthByteSize]))
	stringLength = st.minLength + (stringLength % st.offsetFromMinLen)

	resBytes = resBytes[stringLengthByteSize:]

	// We are looping by the returned random bytes. If the size is higher than the length of the random bytes,
	// we start from the beginning

	// realIdx - real position in random bytes
	realIdx := 0

	subIter := 0

	for idx := 0; idx < stringLength; idx++ {
		if realIdx >= len(resBytes)-1 {
			realIdx = 0
			subIter++
		}
		startPos := realIdx * st.requiredBytesPerCharLength
		endPos := realIdx*st.requiredBytesPerCharLength + st.requiredBytesPerCharLength

		charIdx := (getInt(resBytes[startPos:endPos]) + subIter) % len(st.characters)
		st.buf[idx] = st.characters[charIdx]
		realIdx += st.requiredBytesPerCharLength
	}

	return st.buf[:stringLength]
}

func (st *RandomStringTransformer) GetRequiredGeneratorByteLength() int {
	return st.byteLength
}

func (st *RandomStringTransformer) SetGenerator(g generators.Generator) error {
	if g.Size() < st.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", st.byteLength, g.Size())
	}
	st.generator = g
	return nil
}

func getInt(data []byte) int {
	if len(data) > 1 {
		return int(binary.LittleEndian.Uint16(data[:2]))
	}
	return int(data[0])
}
