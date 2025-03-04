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
