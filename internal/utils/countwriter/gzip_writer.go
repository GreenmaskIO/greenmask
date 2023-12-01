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

package countwriter

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"
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
