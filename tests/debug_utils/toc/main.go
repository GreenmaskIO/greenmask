// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"path"

	"github.com/rs/zerolog/log"

	toclib "github.com/greenmaskio/greenmask/internal/db/postgres/toc"
)

func main() {
	dirPath := os.Args[1]
	src, err := os.Open(path.Join(dirPath, "toc.dat"))
	if err != nil {
		log.Fatal().Err(err).Msg("error")
	}
	defer src.Close()
	dest, err := os.Create(path.Join(dirPath, "new_toc2.dat"))
	if err != nil {
		log.Fatal().Err(err).Msg("error")
	}
	defer dest.Close()

	reader := toclib.NewReader(src)
	toc, err := reader.Read()
	if err != nil {
		log.Fatal().Err(err).Msgf("err")
	}

	for _, item := range toc.Entries {
		if item.Section == toclib.SectionData {
			log.Printf("%+v\n", item)
		}
	}

	writer := toclib.NewWriter(dest)
	if err := writer.Write(toc); err != nil {
		log.Fatal().Err(err).Msgf("err")
	}
}
