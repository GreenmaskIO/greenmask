package custom

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
)

func lineReader(ctx context.Context, r io.Reader, lineHook func(line []byte) error) error {
	lineScanner := bufio.NewReader(r)
	defer func() {
		for {
			line, _, err := lineScanner.ReadLine()
			if err != nil {
				return
			}
			if err := lineHook(line); err != nil {
				return
			}
		}
	}()

	for {
		line, _, err := lineScanner.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
				return nil
			}
			return err
		}

		if err := lineHook(line); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}
