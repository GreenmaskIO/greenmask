package main

import (
	"encoding/binary"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
)

func main() {
	// Open the TOC file for reading
	byteSize := 4
	intData := make([]byte, byteSize)
	magicStrData := make([]byte, 5)
	versionData := make([]byte, 3)
	var major, minor, rev byte

	file, err := os.Open("/tmp/pg_dump_test/toc.dat")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	//scanner := bufio.NewScanner(file)

	// Create a slice to hold the TOC entries

	if _, err := file.Read(magicStrData); err != nil {
		log.Err(err)
	}
	log.Printf("%s", magicStrData)

	if _, err := file.Read(versionData); err != nil {
		log.Err(err)
	}
	log.Printf("%s", versionData)
	major = versionData[0]
	minor = versionData[1]
	rev = versionData[2]
	log.Printf("%d\n", int(major)*256+int(minor)*256+int(rev))

	intSizeByte := make([]byte, 1)

	if _, err := file.Read(intSizeByte); err != nil {
		log.Err(err)
	}
	log.Printf("%s", intSizeByte)

	intSizeByte := make([]byte, 1)

	// Loop through the lines in the file
	for {
		if _, err := file.Read(intData); err != nil {
			log.Err(err)
		}
		log.Printf("%d", binary.LittleEndian.Uint32(intData))

	}

}
