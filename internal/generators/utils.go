package generators

import "encoding/binary"

func BuildBytesFromInt64(value int64) []byte {
	res := make([]byte, 8)
	binary.LittleEndian.PutUint64(res, uint64(value))
	return res
}

// BuildInt64FromBytes - decode bytes array to int64 representation. In case there is less
func BuildInt64FromBytes(data []byte) (res int64) {
	intBytes := data
	if len(data) != 8 {
		intBytes = make([]byte, 8)
		copy(intBytes, data[:8])
	}

	return int64(binary.LittleEndian.Uint64(intBytes))
}

func BuildBytesFromUint64(value uint64) []byte {
	res := make([]byte, 8)
	binary.LittleEndian.PutUint64(res, value)
	return res
}

func BuildUint64FromBytes(data []byte) (res uint64) {
	intBytes := data
	if len(data) != 8 {
		intBytes = make([]byte, 8)
		copy(intBytes, data[:8])
	}

	return binary.LittleEndian.Uint64(intBytes)
}
