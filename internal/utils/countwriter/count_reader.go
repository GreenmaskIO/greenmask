package countwriter

import "io"

type CountReadCloser interface {
	GetCount() int64
	io.ReadCloser
}

type Reader struct {
	r     io.ReadCloser
	Count int64
}

func NewReader(r io.ReadCloser) *Reader {
	return &Reader{
		r: r,
	}
}

func (r *Reader) Read(p []byte) (n int, err error) {
	c, err := r.r.Read(p)
	r.Count += int64(c)
	return c, err
}

func (r *Reader) Close() error {
	return r.r.Close()
}

func (r *Reader) GetCount() int64 {
	return r.Count
}
