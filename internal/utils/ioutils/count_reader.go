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

package ioutils

import "io"

type CountReadCloser interface {
	GetCount() int64
	io.ReadCloser
}

type Reader struct {
	r     io.ReadCloser
	Count int64
}

func NewReader(r io.ReadCloser) *Reader {
	return &Reader{
		r: r,
	}
}

func (r *Reader) Read(p []byte) (n int, err error) {
	c, err := r.r.Read(p)
	r.Count += int64(c)
	return c, err
}

func (r *Reader) Close() error {
	return r.r.Close()
}

func (r *Reader) GetCount() int64 {
	return r.Count
}
