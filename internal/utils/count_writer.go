package utils

import "io"

type CountWriter struct {
	w     io.Writer
	Count int64
}

func NewCountWriter(w io.Writer) *CountWriter {
	return &CountWriter{
		w: w,
	}
}

func (cw *CountWriter) Write(p []byte) (int, error) {
	c, err := cw.w.Write(p)
	cw.Count += int64(c)
	return c, err
}
