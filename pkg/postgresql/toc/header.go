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

import "time"

type CompressionSpecification struct {
	Algorithm int32
	Options   uint32
	Level     int32
	Workers   int32
}

type Crtm struct {
	TmSec   int32
	TmMin   int32
	TmHour  int32
	TmMday  int32
	TmMon   int32
	TmYear  int32
	TmIsDst int32
}

func (c *Crtm) Time() time.Time {
	return time.Date(1900+int(c.TmYear), time.Month(c.TmMon+1), int(c.TmMday), int(c.TmHour), int(c.TmMin), int(c.TmSec), 0, time.Local)
}

type Header struct {
	VersionMajor byte
	VersionMinor byte
	VersionRev   byte
	Version      int /* Version of file */
	IntSize      uint32
	// TODO: How affects that offset size for reading the file
	OffSize              uint32 /* Size of a file offset in the archive - Added V1.7 */
	Format               byte
	CompressionSpec      CompressionSpecification
	CrtmDateTime         Crtm
	ArchDbName           *string
	ArchiveRemoteVersion *string /* When reading an archive, the version of the dumped DB */
	ArchiveDumpVersion   *string /* When reading an archive, the version of the dumper */
	TocCount             int32
	MaxDumpId            int32
}

func (h *Header) Copy() *Header {
	res := NewObj(*h)
	if h.ArchDbName != nil {
		res.ArchDbName = NewObj(*h.ArchDbName)
	}
	if h.ArchiveRemoteVersion != nil {
		res.ArchiveRemoteVersion = NewObj(*h.ArchiveRemoteVersion)
	}
	if h.ArchiveDumpVersion != nil {
		res.ArchiveDumpVersion = NewObj(*h.ArchiveDumpVersion)
	}
	return res
}
