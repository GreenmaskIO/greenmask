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

package testutils

import (
	"io"

	"github.com/stretchr/testify/mock"
)

type ReadWriteCloserMock struct {
	mock.Mock
}

func NewReadWriteCloserMock() *ReadWriteCloserMock {
	return &ReadWriteCloserMock{}
}

func (b *ReadWriteCloserMock) Read(p []byte) (n int, err error) {
	// TODO: It's too complicated. Get rid of the 2nd argument.
	args := b.Called(p)
	if args.Get(1) != nil {
		return args.Int(0), args.Error(1)
	}
	count := copy(p, args.Get(2).([]byte))
	return count, io.EOF
}

func (b *ReadWriteCloserMock) Write(p []byte) (n int, err error) {
	args := b.Called(p)
	if args.Get(0) == nil {
		return args.Int(0), args.Error(1)
	}
	return args.Int(0), args.Error(1)
}

func (b *ReadWriteCloserMock) Close() error {
	args := b.Called()
	return args.Error(0)
}
