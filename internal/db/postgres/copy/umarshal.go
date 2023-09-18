package copy

const defaultCopyDelimiter = '\t'

var defaultNullSeq = []byte("\\N")
var defaultCopyTerminationSeq = []byte("\\.")

type Pos struct {
	start int
	end   int
}

type CopyRow struct {
	positions []Pos
	data      []byte
}

//
//func (cr *CopyRow) GetColumn(idx int) ([]byte, error) {
//
//}
//
//func (cr *CopyRow) SetColumn(idx int, v []byte) error {
//
//}

// Decoder - decode from transfer representation to real type
// decodeAttr - decode attr from raw []byte to unescaped []byte
// CopyAttributeOutText
//func decodeAttr(raw []byte) ([]byte, error) {
//	for i := 0; i < len(raw); i++ {
//		c := raw[i]
//		if raw[i] < 0x20 {
//			switch raw[i] {
//			case '\b':
//				c = 'b'
//			case '\f':
//				c = 'f'
//			case '\n':
//				c = 'n'
//				break
//			case '\r':
//				c = 'r'
//			case '\t':
//				c = 't'
//			case '\v':
//				c = 'v'
//			}
//		} else if raw[i] == '\\' || raw[i] == defaultCopyDelimiter {
//
//		} else {
//
//		}
//	}
//}
