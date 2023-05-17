package main

import (
	"fmt"
	"regexp"
)

func main() {
	r := regexp.MustCompile(`(?P<Year>\d{4})-(?P<Month>\d{2})-(?P<Day>\d{2})`)
	fmt.Printf("%#v\n", r.FindStringSubmatch(`2015-05-27`))
	fmt.Printf("%#v\n", r.SubexpNames())
	id := r.SubexpIndex("Month")
	fmt.Println(r.FindStringSubmatch(``))
	fmt.Println(id)
}
