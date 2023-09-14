package countwriter

import (
	"compress/gzip"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
)

type GzipWriter struct {
	w  io.WriteCloser
	gz *gzip.Writer
}

func NewGzipWriter(w io.WriteCloser) *GzipWriter {
	return &GzipWriter{
		w:  w,
		gz: gzip.NewWriter(w),
	}
}

func (gw *GzipWriter) Write(p []byte) (int, error) {
	return gw.gz.Write(p)
}

// Close - closing method with gz buffer flushing
func (gw *GzipWriter) Close() error {
	defer gw.w.Close()
	flushErr := gw.gz.Flush()
	if flushErr != nil {
		log.Warn().Err(flushErr).Msg("error flushing gzip buffer")
	}
	if closeErr := gw.gz.Close(); closeErr != nil || flushErr != nil {
		err := closeErr
		if flushErr != nil {
			err = flushErr
		}
		return fmt.Errorf("error closing gzip writer: %w", err)
	}
	return nil
}
