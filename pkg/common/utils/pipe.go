// Copyright 2023 Greenmask
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
	"bufio"
	"fmt"
	"io"
)

// pipeBufferSize is the buffer placed in front of the synchronous io.Pipe so
// that many small per-row writes are coalesced into a few large writes. Without
// it, each row triggers a full producer->consumer goroutine handoff on the
// unbuffered pipe, which dominates dump time. Matches pgzip's default block
// size so the plain path performs on par with the compressed one.
const pipeBufferSize = 256 << 10

// NewGzipPipe - returns wrapped PipeWriter into (GzipWriter && Writer) and PipeReader into (Reader)
func NewGzipPipe(usePgzip bool) (CountWriteCloser, CountReadCloser) {
	pr, pw := io.Pipe()
	// Wrapping writer pipe into count writer and gzip writer and reader pipe
	// into count reader. The gzip/pgzip writer buffers internally, so no extra
	// bufio layer is needed here.
	return NewWriter(NewGzipWriter(pw, usePgzip)), NewReader(pr)
}

func NewPlainPipe() (CountWriteCloser, CountReadCloser) {
	pr, pw := io.Pipe()
	// Wrapping writer pipe into count writer and reader pipe into count reader.
	// A bufio layer in front of the pipe coalesces tiny per-row writes so the
	// synchronous io.Pipe isn't hit once per row.
	return NewWriter(newBufferedWriteCloser(pw, pipeBufferSize)), NewReader(pr)
}

// bufferedWriteCloser wraps an io.WriteCloser with a bufio.Writer, flushing the
// buffer before closing the underlying writer.
type bufferedWriteCloser struct {
	bw *bufio.Writer
	wc io.WriteCloser
}

func newBufferedWriteCloser(wc io.WriteCloser, size int) *bufferedWriteCloser {
	return &bufferedWriteCloser{
		bw: bufio.NewWriterSize(wc, size),
		wc: wc,
	}
}

func (b *bufferedWriteCloser) Write(p []byte) (int, error) {
	return b.bw.Write(p)
}

// Close flushes any buffered data and closes the underlying writer. The
// underlying writer is closed even when the flush fails so the reader side of
// the pipe is always unblocked.
func (b *bufferedWriteCloser) Close() error {
	if err := b.bw.Flush(); err != nil {
		_ = b.wc.Close()
		return fmt.Errorf("flush buffer: %w", err)
	}
	return b.wc.Close()
}
