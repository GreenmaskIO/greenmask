package countwriter

import (
	"io"
)

// NewGzipPipe - returns wrapped PipeWriter into (GzipWriter && Writer) and PipeReader into (Reader)
func NewGzipPipe() (CountWriteCloser, CountReadCloser) {
	pr, pw := io.Pipe()
	// Wrapping writer pipe into count writer and gzip writer and reader pipe
	// into count reader
	return NewWriter(NewGzipWriter(pw)), NewReader(pr)
}
