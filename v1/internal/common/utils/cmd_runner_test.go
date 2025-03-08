package utils

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type readerMock struct {
	mock.Mock
	bytesWritten int
}

func (e *readerMock) Read(p []byte) (n int, err error) {
	args := e.Called(p)
	dataToWrite := args.Get(0).([]byte)
	if e.bytesWritten+len(dataToWrite) > len(p) {
		dataToWrite = dataToWrite[:len(p)-e.bytesWritten]
	}
	copy(p[e.bytesWritten:], dataToWrite)
	e.bytesWritten += len(dataToWrite)
	return len(dataToWrite), args.Error(1)
}

type writerMock struct {
	mock.Mock
}

func (s *writerMock) Write(p []byte) (n int, err error) {
	args := s.Called(p)
	return len(p), args.Error(0)
}

func TestCmdRunner_ExecuteCmdAndWriteStdout(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		ctx := context.Background()
		text := `Lorem ipsum dolor sit amet, consectetur
adipiscing elit, sed do eiusmod tempor incididunt
ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor
in reprehenderit in voluptate velit esse cillum
dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt
mollit anim id est laborum.`
		cmd := NewCmdRunner("echo", []string{text})
		writer := bytes.NewBuffer(nil)
		err := cmd.ExecuteCmdAndWriteStdout(ctx, writer)
		require.NoError(t, err)
		assert.Equal(t, text+"\n", writer.String())
	})

	t.Run("error on start", func(t *testing.T) {
		ctx := context.Background()
		cmd := NewCmdRunner("unknown-command-123", nil)
		writer := bytes.NewBuffer(nil)
		err := cmd.ExecuteCmdAndWriteStdout(ctx, writer)
		require.ErrorContains(t, err, "start external command")
	})

	t.Run("context canceled on the start", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		cmd := NewCmdRunner("echo", []string{"test"})
		writer := bytes.NewBuffer(nil)
		err := cmd.ExecuteCmdAndWriteStdout(ctx, writer)
		require.ErrorContains(t, err, "start external command")
	})

	t.Run("context canceled in runtime", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cmd := NewCmdRunner("sleep", []string{"10"})
		writer := bytes.NewBuffer(nil)
		go func() {
			// Wait for the command to start
			<-time.After(1 * time.Second)
			cancel()
		}()
		err := cmd.ExecuteCmdAndWriteStdout(ctx, writer)
		require.ErrorContains(t, err, "killed")
	})
}

func TestCmdRunner_listenStderrAndLog(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		var buf bytes.Buffer
		// Create a zerolog logger with the buffer as output
		ctx := zerolog.New(&buf).With().Timestamp().Logger().WithContext(context.Background())
		text := `Lorem ipsum dolor sit amet, consectetur 
adipiscing elit, sed do eiusmod tempor incididunt 
ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor
in reprehenderit in voluptate velit esse cillum
dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt 
mollit anim id est laborum.`
		stderrReader := bytes.NewBuffer(nil)
		_, err := stderrReader.Write([]byte(text))
		require.NoError(t, err)

		cmd := NewCmdRunner("echo", nil)
		err = cmd.listenStderrAndLog(ctx, stderrReader)
		require.NoError(t, err)

		// Check if the buffer contains the expected log message
		lines := strings.Split(text, "\n")
		logLine := buf.String()
		for idx := range lines {
			assert.Contains(t, logLine, lines[idx], "log message %d should contain the line", idx)
		}
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		mockStderr := &readerMock{}
		text := `Lorem ipsum dolor sit amet, consectetur 
adipiscing elit, sed do eiusmod tempor incididunt`
		mockStderr.On("Read", mock.Anything).
			Run(func(args mock.Arguments) {
				cancel()
			}).Return([]byte(text), io.EOF)
		cmd := NewCmdRunner("echo", nil)
		err := cmd.listenStderrAndLog(ctx, mockStderr)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("non EOF error", func(t *testing.T) {
		ctx := context.Background()
		mockStderr := &readerMock{}
		text := `Lorem ipsum dolor sit amet, consectetur 
adipiscing elit, sed do eiusmod tempor incididunt`
		mockStderr.On("Read", mock.Anything).
			Return([]byte(text), errors.New("some error"))
		cmd := NewCmdRunner("echo", nil)
		err := cmd.listenStderrAndLog(ctx, mockStderr)
		require.ErrorContains(t, err, "some error")
	})
}

func TestCmdRunner_listenStdoutAndWrite(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		ctx := context.Background()
		text := `Lorem ipsum dolor sit amet, consectetur 
adipiscing elit, sed do eiusmod tempor incididunt 
ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor
in reprehenderit in voluptate velit esse cillum
dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt 
mollit anim id est laborum.`
		stdoutReader := bytes.NewBuffer(nil)
		_, err := stdoutReader.Write([]byte(text))
		require.NoError(t, err)
		writer := bytes.NewBuffer(nil)
		cmd := NewCmdRunner("echo", nil)
		err = cmd.listenStdoutAndWrite(ctx, stdoutReader, writer)
		require.NoError(t, err)
		assert.Equal(t, text, writer.String())
	})

	t.Run("error on write", func(t *testing.T) {
		ctx := context.Background()
		text := `Lorem ipsum dolor sit amet, consectetur 
adipiscing elit, sed do eiusmod tempor incididunt 
ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor
in reprehenderit in voluptate velit esse cillum
dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt 
mollit anim id est laborum.`
		stdoutReader := bytes.NewBuffer(nil)
		_, err := stdoutReader.Write([]byte(text))
		require.NoError(t, err)
		writer := &writerMock{}
		writer.On("Write", mock.Anything).Return(errors.New("some error"))
		cmd := NewCmdRunner("echo", nil)
		err = cmd.listenStdoutAndWrite(ctx, stdoutReader, writer)
		require.ErrorContains(t, err, "some error")
	})

	t.Run("error non EOF on read", func(t *testing.T) {
		ctx := context.Background()
		text := `Lorem ipsum dolor sit amet, consectetur`
		stdoutReader := &readerMock{}
		stdoutReader.On("Read", mock.Anything).
			Return([]byte(text), errors.New("some error"))

		writer := &writerMock{}
		writer.On("Write", mock.Anything).Return(nil)
		cmd := NewCmdRunner("echo", nil)
		err := cmd.listenStdoutAndWrite(ctx, stdoutReader, writer)
		require.ErrorContains(t, err, "some error")
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		cmd := NewCmdRunner("echo", nil)
		err := cmd.listenStdoutAndWrite(ctx, &readerMock{}, &writerMock{})
		require.ErrorIs(t, err, context.Canceled)
	})
}
