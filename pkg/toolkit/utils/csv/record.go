package csv

type Record struct {
	Buf [][]byte
}

func (r *Record) GetFieldByIdx(idx int) []byte {
	return r.Buf[idx]
}

func (r *Record) SetFieldByIdx(idx int, v []byte) {
	r.Buf[idx] = v
}
