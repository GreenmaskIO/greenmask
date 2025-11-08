// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
