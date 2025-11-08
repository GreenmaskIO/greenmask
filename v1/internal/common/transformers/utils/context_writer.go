package utils

import (
	"context"
	"io"
)

type ContextWriter interface {
	WriteContext(ctx context.Context, p []byte) (n int, err error)
	WithContext(ctx context.Context) io.Writer
	Write(p []byte) (n int, err error)
}

type DefaultContextWriter struct {
	w   io.Writer
	ctx context.Context
}

func NewDefaultContextWriter(w io.Writer) *DefaultContextWriter {
	return &DefaultContextWriter{w: w}
}

func (d *DefaultContextWriter) WriteContext(ctx context.Context, p []byte) (int, error) {
	done := make(chan readWriterResults, 1)
	go func() {
		n, err := d.w.Write(p)
		done <- readWriterResults{
			n:   n,
			err: err,
		}
	}()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case res := <-done:
		return res.n, res.err
	}
}

func (d *DefaultContextWriter) WithContext(ctx context.Context) io.Writer {
	d.ctx = ctx
	return d
}

func (d *DefaultContextWriter) Write(p []byte) (n int, err error) {
	return d.WriteContext(d.ctx, p)
}
