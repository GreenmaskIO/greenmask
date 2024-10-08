package ioutils

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/klauspost/pgzip"
)

// GetGzipReadCloser - returns a gzip or pgzip reader
func GetGzipReadCloser(r io.Reader, usePgzip bool) (gz io.ReadCloser, err error) {
	if usePgzip {
		gz, err = pgzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("cannot create pgzip reader: %w", err)
		}
	} else {
		gz, err = gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("cannot create gzip reader: %w", err)
		}
	}
	return gz, nil
}
