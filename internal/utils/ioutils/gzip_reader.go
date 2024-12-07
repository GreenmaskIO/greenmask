package ioutils

import (
	"fmt"
	"io"

	"github.com/rs/zerolog/log"
)

type GzipReader struct {
	gz io.ReadCloser
	r  io.ReadCloser
}

func NewGzipReader(r io.ReadCloser, usePgzip bool) (*GzipReader, error) {
	gz, err := GetGzipReadCloser(r, usePgzip)
	if err != nil {
		if err := r.Close(); err != nil {
			log.Warn().
				Err(err).
				Msg("error closing dump file")
		}
		return nil, fmt.Errorf("cannot create gzip reader: %w", err)
	}

	return &GzipReader{
		gz: gz,
		r:  r,
	}, nil

}

func (r *GzipReader) Read(p []byte) (n int, err error) {
	return r.gz.Read(p)
}

func (r *GzipReader) Close() error {
	var lastErr error
	if err := r.gz.Close(); err != nil {
		lastErr = fmt.Errorf("error closing gzip reader: %w", err)
		log.Warn().
			Err(err).
			Msg("error closing gzip reader")
	}
	if err := r.r.Close(); err != nil {
		lastErr = fmt.Errorf("error closing dump file: %w", err)
		log.Warn().
			Err(err).
			Msg("error closing dump file")
	}
	return lastErr
}
