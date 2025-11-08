package utils

import (
	"context"
	"io"
)

type ContextReader interface {
	ReadContext(ctx context.Context, p []byte) (n int, err error)
	WithContext(ctx context.Context) io.Reader
	Read(p []byte) (n int, err error)
}

type readWriterResults struct {
	n   int
	err error
}

type DefaultContextReader struct {
	r   io.Reader
	ctx context.Context
}

func NewDefaultContextReader(r io.Reader) *DefaultContextReader {
	return &DefaultContextReader{r: r}
}

func (d *DefaultContextReader) ReadContext(ctx context.Context, p []byte) (int, error) {
	done := make(chan readWriterResults, 1)
	go func() {
		n, err := d.r.Read(p)
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

func (d *DefaultContextReader) WithContext(ctx context.Context) io.Reader {
	d.ctx = ctx
	return d
}

func (d *DefaultContextReader) Read(p []byte) (n int, err error) {
	return d.ReadContext(d.ctx, p)
}
