package strings

import (
	"strings"

	"github.com/mitchellh/go-wordwrap"
)

func WrapString(v string, maxLength int) string {
	strs := strings.Split(wordwrap.WrapString(v, uint(maxLength)), "\n")
	res := make([]string, 0, len(strs))
	for _, s := range strs {
		if len(s) > maxLength {

			for idx := 0; idx < len(s); idx += maxLength {
				rest := idx + maxLength
				if rest > len(s) {
					rest = idx + len(s) - idx
				}
				res = append(res, s[idx:rest])
			}

		} else {
			res = append(res, s)
		}
	}
	return strings.Join(res, "\n")
}
