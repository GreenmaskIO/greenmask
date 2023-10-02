package custom

import (
	"bufio"
	"context"
	"errors"
	"github.com/rs/zerolog/log"
	"io"
	"os"
)

func lineReader(ctx context.Context, r io.Reader, lineHook func(line []byte) error) error {
	lineScanner := bufio.NewReader(r)
	for {
		line, _, err := lineScanner.ReadLine()
		if err != nil {
			log.Debug().Err(err).Msg("line reader error")
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
