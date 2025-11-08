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
