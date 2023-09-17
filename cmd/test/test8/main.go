package main

import (
	"encoding/hex"
	"fmt"

	"github.com/sergi/go-diff/diffmatchpatch"
)

var (
	text1 = []byte("Lorem ipsum dolor.")
	text2 = []byte("Lorem dolor sit amet.")
)

func main() {
	dmp := diffmatchpatch.New()

	diffs := dmp.DiffMain(hex.EncodeToString(text1), hex.EncodeToString(text2), false)

	fmt.Println(dmp.DiffPrettyHtml(diffs))

	s := "in the middle"
	w := 110 // or whatever

	fstCol := fmt.Sprintf("%[1]*s", -w, fmt.Sprintf("%[1]*s", (w+len(s))/2, s))
	scndCol := fmt.Sprintf("%[1]*s", -w, fmt.Sprintf("%[1]*s", (w+len(s))/2, s))
	fmt.Printf("%s|%s", fstCol, scndCol)
}
