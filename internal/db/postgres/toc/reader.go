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

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"

	"github.com/rs/zerolog/log"
)

type Reader struct {
	r         io.Reader
	intSize   uint32
	version   int
	position  int
	maxDumpId int32
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r: r,
	}
}

func (r *Reader) prune() {
	r.intSize = 0
	r.version = 0
	r.position = 0
	r.maxDumpId = 0
}

func (r *Reader) Read() (*Toc, error) {
	defer r.prune()
	header, err := r.readHeader()
	if err != nil {
		return nil, fmt.Errorf("error reading header: %w", err)
	}

	entries, err := r.readEntries()
	if err != nil {
		return nil, fmt.Errorf("error reading entries: %w", err)
	}
	header.TocCount = int32(len(entries))
	header.MaxDumpId = r.maxDumpId
	return &Toc{
		Header:  header,
		Entries: entries,
	}, nil
}

func (r *Reader) readStr() (*string, error) {
	l, err := r.readInt()
	if err != nil {
		return nil, err
	}
	if l < 0 {
		return nil, nil
	}

	buf := make([]byte, l)

	n, err := r.r.Read(buf)
	if err != nil {
		return nil, err
	}
	r.position += n
	strVal := string(buf)
	return &strVal, nil
}

func (r *Reader) readInt() (int32, error) {
	var sign byte = 0
	var err error
	var res, bitShift int32

	if r.intSize != 4 {
		return 0, errors.New("unsupported int32 size")
	}

	if r.version == 0 {
		return 0, errors.New("version cannot be 0")
	}

	if r.version > BackupVersions["1.0"] {
		sign, err = r.readByte()
		if err != nil {
			return 0, fmt.Errorf("cannot read srcFile byte: %s", err)
		}
	}

	intBytes := make([]byte, r.intSize)
	n, err := r.r.Read(intBytes)
	if err != nil {
		return 0, err
	}
	r.position += n

	for _, b := range intBytes {
		bv := b & 0xFF
		if bv != 0 {
			res = res + (int32(bv) << bitShift)
		}
		bitShift += 8
	}

	if sign != 0 {
		return -res, nil
	}
	return res, nil
}

func (r *Reader) readByte() (byte, error) {
	res, err := r.readBytes(1)
	if err != nil {
		return 0, err
	}
	return res[0], nil
}

func (r *Reader) readBytes(length int) ([]byte, error) {
	bytes := make([]byte, length)
	n, err := r.r.Read(bytes)
	if err != nil {
		return nil, err
	}
	r.position += n
	return bytes, nil
}

func (r *Reader) scanBytes(byteVars ...*byte) error {
	bytes, err := r.readBytes(len(byteVars))
	if err != nil {
		return err
	}
	for idx := range bytes {
		*byteVars[idx] = bytes[idx]
	}

	return nil
}

func (r *Reader) scanInt(byteVars ...*int32) error {

	for idx := range byteVars {
		val, err := r.readInt()
		if err != nil {
			return err
		}
		*byteVars[idx] = val
	}

	return nil
}

func (r *Reader) readHeader() (*Header, error) {
	header := &Header{}
	magicString, err := r.readBytes(5)
	if err != nil {
		log.Err(err)
	}
	if string(magicString) != "PGDMP" {
		return nil, errors.New("did not find magic string in srcFile handler")
	}
	if err = r.scanBytes(&header.VersionMajor, &header.VersionMinor); err != nil {
		return nil, fmt.Errorf("unable to scan major and minor version data: %w", err)
	}

	if header.VersionMajor > 1 || (header.VersionMajor == 1 && header.VersionMinor > 0) {
		if err = r.scanBytes(&header.VersionRev); err != nil {
			return nil, fmt.Errorf("unable to scan rev version data: %w", err)
		}
	}

	header.Version = MakeArchiveVersion(header.VersionMajor, header.VersionMinor, header.VersionRev)
	r.version = header.Version

	if header.Version < BackupVersions["1.0"] || header.Version > BackupVersions[MaxVersion] {
		return nil, fmt.Errorf("unsupported archive version %d.%d", header.VersionMajor, header.VersionMinor)
	}

	intSize, err := r.readByte()
	if err != nil {
		return nil, fmt.Errorf("cannot read intSize value: %w", err)
	}
	header.IntSize = uint32(intSize)
	if intSize > 32 {
		return nil, fmt.Errorf("sanity check on integer size %d failed", header.IntSize)
	}
	r.intSize = header.IntSize

	if header.Version >= BackupVersions["1.7"] {
		offSize, err := r.readByte()
		if err != nil {
			return nil, fmt.Errorf("cannot read intSize value: %w", err)
		}
		header.OffSize = uint32(offSize)
	} else {
		header.OffSize = header.IntSize
	}

	if err := r.scanBytes(&header.Format); err != nil {
		return nil, fmt.Errorf("unable to scan bytes from TOC srcFile: %w", err)
	}

	/*
	 * Write 'tar' in the format field of the toc.dat file. The directory
	 * is compatible with 'tar', so there's no point having a different
	 * format code for it.
	 */
	if ArchTar != header.Format {
		return nil, fmt.Errorf("unsupported format \"%s\": suports only directory", BackupFormats[header.Format])
	}

	if header.Version >= BackupVersions["1.15"] {
		algorithm, err := r.readByte()
		if err != nil {
			return nil, fmt.Errorf("unable to scan CompressionSpec.Algorithm: %w", err)
		}
		header.CompressionSpec.Algorithm = int32(algorithm)
	} else if header.Version >= BackupVersions["1.2"] {
		if header.Version < BackupVersions["1.4"] {
			level, err := r.readByte()
			if err != nil {
				return nil, fmt.Errorf("unable to scan CompressionSpec.Severity: %w", err)
			}
			header.CompressionSpec.Level = int32(level)
		} else {
			if err = r.scanInt(&header.CompressionSpec.Level); err != nil {
				return nil, fmt.Errorf("unable to scan CompressionSpec.Severity: %w", err)
			}
		}
		if header.CompressionSpec.Level != 0 {
			header.CompressionSpec.Algorithm = PgCompressionGzip
		}
	} else {
		header.CompressionSpec.Level = PgCompressionGzip
	}

	// TODO: Ensure we support compression specification

	if header.Version >= BackupVersions["1.4"] {
		var tmSec, tmMin, tmHour, tmDay, tmMon, tmYear, tmIsDst int32
		if err = r.scanInt(&tmSec, &tmMin, &tmHour, &tmDay, &tmMon, &tmYear, &tmIsDst); err != nil {
			return nil, fmt.Errorf("cannot scan backup date: %w", err)
		}
		header.CrtmDateTime = Crtm{
			TmSec:   tmSec,
			TmMin:   tmMin,
			TmHour:  tmHour,
			TmMday:  tmDay,
			TmMon:   tmMon,
			TmYear:  tmYear,
			TmIsDst: tmIsDst,
		}
	}

	if header.Version >= BackupVersions["1.4"] {
		archDbName, err := r.readStr()
		if err != nil {
			return nil, fmt.Errorf("cannot read archdbname: %w", err)
		}
		header.ArchDbName = archDbName
	}

	if header.Version >= BackupVersions["1.10"] {
		archiveRemoteVersion, err := r.readStr()
		if err != nil {
			return nil, fmt.Errorf("cannot rad archiveRemoteVersion: %w", err)
		}
		header.ArchiveRemoteVersion = archiveRemoteVersion

		archiveDumpVersion, err := r.readStr()
		if err != nil {
			return nil, fmt.Errorf("cannot read archiveDumpVersion: %w", err)
		}
		header.ArchiveDumpVersion = archiveDumpVersion
	}

	return header, nil
}

func (r *Reader) readEntries() ([]*Entry, error) {
	var tocCount int32
	if err := r.scanInt(&tocCount); err != nil {
		return nil, fmt.Errorf("cannot scan tocCount: %w", err)
	}

	//var maxDumpId int32

	entries := make([]*Entry, 0, tocCount)

	for i := int32(0); i < tocCount; i++ {
		entry := Entry{}
		if err := r.scanInt(&entry.DumpId); err != nil {
			return nil, fmt.Errorf("cannot scan tocCount: %w", err)
		}

		if r.maxDumpId < entry.DumpId {
			r.maxDumpId = entry.DumpId
		}

		if entry.DumpId <= 0 {
			return nil, fmt.Errorf("entry ID %d out of range perhaps a corrupt TOC", entry.DumpId)
		}

		if err := r.scanInt(&entry.HadDumper); err != nil {
			return nil, fmt.Errorf("cannot scan hadDumer data: %w", err)
		}

		if r.version >= BackupVersions["1.8"] {
			tmp, err := r.readStr()
			if err != nil {
				return nil, fmt.Errorf("cannot read CatalogId: %w", err)
			}
			if tmp == nil {
				return nil, errors.New("unexpected nil pointer")
			}
			tableOid, err := strconv.ParseUint(*tmp, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("cannot cast str to uint32: %w", err)
			}
			entry.CatalogId.TableOid = Oid(tableOid)
		} else {
			entry.CatalogId.TableOid = InvalidOid
		}
		tmp, err := r.readStr()
		if err != nil {
			return nil, fmt.Errorf("cannot read CatalogId: %w", err)
		}
		if tmp == nil {
			return nil, errors.New("unexpected nil pointer")
		}
		oid, err := strconv.ParseUint(*tmp, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot cast str to uint32: %w", err)
		}
		entry.CatalogId.Oid = Oid(oid)

		tag, err := r.readStr()
		if err != nil {
			return nil, fmt.Errorf("cannot read Tag: %w", err)
		}
		entry.Tag = tag

		desc, err := r.readStr()
		if err != nil {
			return nil, fmt.Errorf("cannot read Desc: %w", err)
		}
		entry.Desc = desc

		if r.version >= BackupVersions["1.11"] {
			if err = r.scanInt(&entry.Section); err != nil {
				return nil, fmt.Errorf("cannot Section: %w", err)
			}
		} else {
			if slices.Contains([]string{"COMMENT", "ACL", "ACL LANGUAGE"}, *entry.Desc) {
				entry.Section = SectionNone
			} else if slices.Contains([]string{"TABLE DATA", "BLOBS", "BLOB COMMENTS"}, *entry.Desc) {
				entry.Section = SectionData
			} else if slices.Contains([]string{
				"CONSTRAINT", "CHECK CONSTRAINT", "FK CONSTRAINT", "INDEX", "RULE", "TRIGGER",
			}, *entry.Desc) {
				entry.Section = SectionPostData
			} else {
				entry.Section = SectionPreData
			}
		}

		defn, err := r.readStr()
		if err != nil {
			return nil, fmt.Errorf("cannot read Defn: %w", err)
		}
		entry.Defn = defn

		dropStmt, err := r.readStr()
		if err != nil {
			return nil, fmt.Errorf("cannot read DropStmt: %w", err)
		}
		entry.DropStmt = dropStmt

		if r.version >= BackupVersions["1.3"] {
			copyStmt, err := r.readStr()
			if err != nil {
				return nil, fmt.Errorf("cannot read Defn: %w", err)
			}
			entry.CopyStmt = copyStmt
		}

		if r.version >= BackupVersions["1.6"] {
			namespace, err := r.readStr()
			if err != nil {
				return nil, fmt.Errorf("cannot read Namespace: %w", err)
			}
			entry.Namespace = namespace
		}

		if r.version >= BackupVersions["1.10"] {
			tablespace, err := r.readStr()
			if err != nil {
				return nil, fmt.Errorf("cannot read Tablespace: %w", err)
			}
			entry.Tablespace = tablespace
		}

		if r.version >= BackupVersions["1.14"] {
			tableam, err := r.readStr()
			if err != nil {
				return nil, fmt.Errorf("cannot read Tableam: %w", err)
			}
			entry.Tableam = tableam
		}

		if r.version >= BackupVersions["1.16"] {
			// The relkind data stores it as int value, but according to the sources only 1 byte is used
			// we can safely cast it to byte
			realKindInt, err := r.readInt()
			if err != nil {
				return nil, fmt.Errorf("cannot read Relkind: %w", err)
			}
			entry.Relkind = byte(realKindInt)
		}

		owner, err := r.readStr()
		if err != nil {
			return nil, fmt.Errorf("cannot read Tablespace: %w", err)
		}
		entry.Owner = owner

		isSupported := true
		if r.version < BackupVersions["1.9"] {
			isSupported = false
		} else {
			tmp, err = r.readStr()
			if err != nil {
				return nil, fmt.Errorf("cannot read CatalogId: %w", err)
			}
			if tmp == nil {
				return nil, errors.New("unexpected nil pointer")
			}
			if *tmp == "true" {
				isSupported = false
			}
		}

		if !isSupported {
			log.Warn().Msg("restoring tables WITH OIDS is not supported anymore")
		}

		/* Read TOC entry Dependencies */
		if r.version >= BackupVersions["1.5"] {
			for {
				tmp, err = r.readStr()
				if err != nil {
					return nil, fmt.Errorf("cannot read CatalogId: %w", err)
				}
				if tmp == nil {
					break
				}

				val, err := strconv.ParseInt(*tmp, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("unable to parse dependency int32 value: %w", err)
				}

				entry.Dependencies = append(entry.Dependencies, int32(val))
			}
			entry.NDeps = int32(len(entry.Dependencies))

		} else {
			entry.Dependencies = nil
			entry.NDeps = 0
		}
		entry.DataLength = 0

		fileName, err := r.readStr()
		if err != nil {
			return nil, fmt.Errorf("cannot read an additional FileName data: %w", err)
		}
		entry.FileName = fileName
		entries = append(entries, &entry)

	}

	return entries, nil
}
