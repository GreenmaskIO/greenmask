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

package toc

const (
	ArchUnknown   byte = 0
	ArchCustom    byte = 1
	ArchTar       byte = 3
	ArchNull      byte = 4
	ArchDirectory byte = 5
)

const InvalidOid = 0

const MaxVersion = "1.16"

const (
	PgCompressionNone int32 = iota
	PgCompressionGzip
	PgCompressionLz4
	PgCompressionZSTD
)

var (
	BackupVersions = map[string]int{
		"1.0":  MakeArchiveVersion(1, 0, 0),
		"1.2":  MakeArchiveVersion(1, 2, 0),
		"1.3":  MakeArchiveVersion(1, 3, 0),
		"1.4":  MakeArchiveVersion(1, 4, 0),
		"1.5":  MakeArchiveVersion(1, 5, 0),
		"1.6":  MakeArchiveVersion(1, 6, 0),
		"1.7":  MakeArchiveVersion(1, 7, 0),
		"1.8":  MakeArchiveVersion(1, 8, 0),
		"1.9":  MakeArchiveVersion(1, 9, 0),
		"1.10": MakeArchiveVersion(1, 10, 0),
		"1.11": MakeArchiveVersion(1, 11, 0),
		"1.12": MakeArchiveVersion(1, 12, 0),
		"1.13": MakeArchiveVersion(1, 13, 0),
		"1.14": MakeArchiveVersion(1, 14, 0),
		"1.15": MakeArchiveVersion(1, 15, 0),
		"1.16": MakeArchiveVersion(1, 16, 0),
	}

	BackupFormats = map[byte]string{
		ArchUnknown:   "unknown",
		ArchCustom:    "custom",
		ArchTar:       "tar",
		ArchNull:      "null",
		ArchDirectory: "directory",
	}
)

func MakeArchiveVersion(major, minor, rev byte) int {
	return (int(major)*256+int(minor))*256 + int(rev)
}
