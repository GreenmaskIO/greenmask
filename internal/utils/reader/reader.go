package reader

import (
	"bufio"
	"fmt"
)

func ReadLine(r *bufio.Reader) ([]byte, error) {
	var res []byte
	for {
		var line []byte
		line, isPrefix, err := r.ReadLine()
		if err != nil {
			return nil, fmt.Errorf("unable to read line: %w", err)
		}
		res = append(res, line...)
		if !isPrefix {
			break
		}
	}
	return res, nil
}
