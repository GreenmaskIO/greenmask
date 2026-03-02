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
