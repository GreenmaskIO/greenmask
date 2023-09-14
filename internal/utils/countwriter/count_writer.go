package countwriter

import "io"

type CountWriteCloser interface {
	GetCount() int64
	io.WriteCloser
}

type Writer struct {
	w     io.WriteCloser
	Count int64
}

func NewWriter(w io.WriteCloser) *Writer {
	return &Writer{
		w: w,
	}
}

func (cw *Writer) Write(p []byte) (int, error) {
	c, err := cw.w.Write(p)
	cw.Count += int64(c)
	return c, err
}

func (cw *Writer) Close() error {
	return cw.w.Close()
}

func (cw *Writer) GetCount() int64 {
	return cw.Count
}
