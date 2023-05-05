package count_writer

import (
	"compress/gzip"
	"io"
)

type GzipWriterWithCount struct {
	afterCompressWriter *CountWriter
	writtenBytes        int64
	gz                  *gzip.Writer
}

func NewGzipWriter(writer io.Writer) *GzipWriterWithCount {
	afterCompressWriter := NewCountWriter(writer)
	gz := gzip.NewWriter(afterCompressWriter)
	return &GzipWriterWithCount{
		afterCompressWriter: afterCompressWriter,
		gz:                  gz,
	}
}

func (gw *GzipWriterWithCount) Write(p []byte) (int, error) {
	n, err := gw.gz.Write(p)
	gw.writtenBytes += int64(n)
	return n, err
}

func (gw *GzipWriterWithCount) Close() error {
	return gw.gz.Close()
}

func (gw *GzipWriterWithCount) Flush() error {
	return gw.gz.Flush()
}

func (gw *GzipWriterWithCount) ReceivedBytes() int64 {
	return gw.writtenBytes
}

func (gw *GzipWriterWithCount) WrittenBytes() int64 {
	return gw.afterCompressWriter.Count
}
