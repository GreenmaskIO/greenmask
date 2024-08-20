package reader

import (
	"bufio"
	"fmt"
)

func ReadLine(r *bufio.Reader, buf []byte) ([]byte, error) {
	buf = buf[:0]
	for {
		var line []byte
		line, isPrefix, err := r.ReadLine()
		if err != nil {
			return nil, fmt.Errorf("unable to read line: %w", err)
		}
		buf = append(buf, line...)
		if !isPrefix {
			break
		}
	}
	return buf, nil
}
