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

import "io"

type CountWriteCloser interface {
	GetCount() int64
	io.WriteCloser
}

type Writer struct {
	w     io.WriteCloser
	Count int64
}

func NewWriter(w io.WriteCloser) *Writer {
	return &Writer{
		w: w,
	}
}

func (cw *Writer) Write(p []byte) (int, error) {
	c, err := cw.w.Write(p)
	cw.Count += int64(c)
	return c, err
}

func (cw *Writer) Close() error {
	return cw.w.Close()
}

func (cw *Writer) GetCount() int64 {
	return cw.Count
}
