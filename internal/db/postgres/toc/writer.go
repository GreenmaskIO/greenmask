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
	"strconv"
)

type Writer struct {
	w        io.Writer
	buf      []byte
	intSize  uint32
	version  int
	position int
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w: w,
	}
}

func (w *Writer) prune() {
	w.buf = w.buf[:]
	w.intSize = 0
	w.version = 0
	w.position = 0
}

func (w *Writer) Write(toc *Toc) error {
	if toc.Entries == nil {
		return errors.New("entries are nil")
	}
	if toc.Header == nil {
		return errors.New("header is nil")
	}
	defer w.prune()
	w.intSize = toc.Header.IntSize
	w.version = toc.Header.Version
	if err := w.writeHeader(toc.Header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	if err := w.writeEntries(toc.Entries); err != nil {
		return fmt.Errorf("error writing entries: %w", err)
	}
	return nil
}

func (w *Writer) writeHeader(header *Header) error {
	if err := w.writeBuf([]byte("PGDMP")); err != nil {
		return fmt.Errorf("cannot write magic str: %w", err)
	}
	if err := w.writeByte(header.VersionMajor); err != nil {
		return fmt.Errorf("cannot write versionMajor: %w", err)
	}

	if err := w.writeByte(header.VersionMinor); err != nil {
		return fmt.Errorf("cannot write versionMinor: %w", err)
	}

	if header.VersionMajor > 1 || (header.VersionMajor == 1 && header.VersionMinor > 0) {
		if err := w.writeByte(header.VersionRev); err != nil {
			return fmt.Errorf("cannot write versionRev: %w", err)
		}
	}

	if err := w.writeByte(byte(header.IntSize)); err != nil {
		return fmt.Errorf("cannot write intSize: %w", err)
	}

	if header.Version >= BackupVersions["1.7"] {
		if err := w.writeByte(byte(header.OffSize)); err != nil {
			return fmt.Errorf("cannot write offSize: %w", err)
		}
	}

	/*
	 * Write 'tar' in the format field of the toc.dat file. The directory
	 * is compatible with 'tar', so there's no point having a different
	 * format code for it.
	 */
	if err := w.writeByte(header.Format); err != nil {
		return fmt.Errorf("cannot write format: %w", err)
	}

	if header.Version >= BackupVersions["1.15"] {
		if err := w.writeByte(byte(header.CompressionSpec.Algorithm)); err != nil {
			return fmt.Errorf("cannot write CompressionSpec.Algorithm: %w", err)
		}
	} else if header.Version >= BackupVersions["1.2"] {
		if header.Version < BackupVersions["1.4"] {
			if err := w.writeByte(byte(header.CompressionSpec.Level)); err != nil {
				return fmt.Errorf("cannot write CompressionSpec.Algorithm: %w", err)
			}
		} else {
			if err := w.writeInt(header.CompressionSpec.Level); err != nil {
				return fmt.Errorf("cannot write CompressionSpec.Algorithm: %w", err)
			}
		}
	}

	if header.Version >= BackupVersions["1.4"] {
		if err := w.writeInt(header.CrtmDateTime.TmSec); err != nil {
			return fmt.Errorf("cannot write TmSec: %w", err)
		}
		if err := w.writeInt(header.CrtmDateTime.TmMin); err != nil {
			return fmt.Errorf("cannot write TmMin: %w", err)
		}
		if err := w.writeInt(header.CrtmDateTime.TmHour); err != nil {
			return fmt.Errorf("cannot write TmHour: %w", err)
		}
		if err := w.writeInt(header.CrtmDateTime.TmMday); err != nil {
			return fmt.Errorf("cannot write TmMday: %w", err)
		}
		if err := w.writeInt(header.CrtmDateTime.TmMon); err != nil {
			return fmt.Errorf("cannot write TmMon: %w", err)
		}
		if err := w.writeInt(header.CrtmDateTime.TmYear); err != nil {
			return fmt.Errorf("cannot write TmYear: %w", err)
		}
		if err := w.writeInt(header.CrtmDateTime.TmIsDst); err != nil {
			return fmt.Errorf("cannot write TmIsDst: %w", err)
		}
	}

	if header.Version >= BackupVersions["1.4"] {
		if err := w.writeStr(header.ArchDbName); err != nil {
			return fmt.Errorf("cannot write archDbName: %w", err)
		}
	}

	if header.Version >= BackupVersions["1.10"] {
		if err := w.writeStr(header.ArchiveRemoteVersion); err != nil {
			return fmt.Errorf("cannot write archiveRemoteVersion: %w", err)
		}
		if err := w.writeStr(header.ArchiveDumpVersion); err != nil {
			return fmt.Errorf("cannot write archiveDumpVersion: %w", err)
		}
	}

	return nil
}

func (w *Writer) writeEntries(entries []*Entry) error {
	var tocCount = int32(len(entries))

	if err := w.writeInt(tocCount); err != nil {
		return fmt.Errorf("cannot write tocCount: %w", err)
	}

	for _, entry := range entries {
		if err := w.writeInt(entry.DumpId); err != nil {
			return fmt.Errorf("unable to write DumpId: %w", err)
		}

		if err := w.writeInt(entry.HadDumper); err != nil {
			return fmt.Errorf("unable to write DataDumper: %w", err)
		}

		if w.version >= BackupVersions["1.8"] {
			oidStr := strconv.FormatUint(uint64(entry.CatalogId.TableOid), 10)
			if err := w.writeStr(&oidStr); err != nil {
				return fmt.Errorf("unable to write TableOid: %s", err)
			}
		}

		oidStr := strconv.FormatUint(uint64(entry.CatalogId.Oid), 10)
		if err := w.writeStr(&oidStr); err != nil {
			return fmt.Errorf("unable to write Name: %w", err)
		}

		if err := w.writeStr(entry.Tag); err != nil {
			return fmt.Errorf("unable to write Tag: %w", err)
		}
		if err := w.writeStr(entry.Desc); err != nil {
			return fmt.Errorf("unable to write Desc: %w", err)
		}

		if w.version >= BackupVersions["1.11"] {
			if err := w.writeInt(entry.Section); err != nil {
				return fmt.Errorf("unable to write Section: %w", err)
			}
		}

		if err := w.writeStr(entry.Defn); err != nil {
			return fmt.Errorf("unable to write Defn: %w", err)
		}
		if err := w.writeStr(entry.DropStmt); err != nil {
			return fmt.Errorf("unable to write DropStmt: %w", err)
		}

		if w.version >= BackupVersions["1.3"] {
			if err := w.writeStr(entry.CopyStmt); err != nil {
				return fmt.Errorf("unable to write CopyStmt: %w", err)
			}
		}

		if w.version >= BackupVersions["1.6"] {
			if err := w.writeStr(entry.Namespace); err != nil {
				return fmt.Errorf("unable to write Namespace: %w", err)
			}
		}

		if w.version >= BackupVersions["1.10"] {
			if err := w.writeStr(entry.Tablespace); err != nil {
				return fmt.Errorf("unable to write Tablespace: %w", err)
			}
		}

		if w.version >= BackupVersions["1.14"] {
			if err := w.writeStr(entry.Tableam); err != nil {
				return fmt.Errorf("unable to write Tableam: %w", err)
			}
		}

		if err := w.writeStr(entry.Owner); err != nil {
			return fmt.Errorf("unable ro write Owner: %w", err)
		}

		if w.version >= BackupVersions["1.9"] {
			tableOidRestoring := "false"
			if err := w.writeStr(&tableOidRestoring); err != nil {
				return fmt.Errorf("unable to write \"false\" value: %w", err)
			}
		}

		if w.version >= BackupVersions["1.5"] {
			for _, d := range entry.Dependencies {
				depStr := strconv.FormatInt(int64(d), 10)
				if err := w.writeStr(&depStr); err != nil {
					return fmt.Errorf("unable to write entry dependency value: %w", err)
				}
			}
			/* Terminate List */
			if err := w.writeStr(nil); err != nil {
				return fmt.Errorf("unable to write entry Dependencies list terminator: %w", err)
			}
		}

		// TODO: Ensure entry.FileName is required for all versions
		// WriteExtraTocPtr - write filename here
		if err := w.writeStr(entry.FileName); err != nil {
			return fmt.Errorf("unable to write FileName: %w", err)
		}

	}

	return nil
}

func (w *Writer) writeBuf(buf []byte) error {
	n, err := w.w.Write(buf)
	if err != nil {
		return err
	}
	w.position += n

	return nil
}

func (w *Writer) writeByte(data byte) error {
	err := w.writeBuf([]byte{data})
	if err != nil {
		return fmt.Errorf("cannot write byte data: %w", err)
	}
	return nil
}

func (w *Writer) writeInt(i int32) error {
	var b int32
	var sign byte

	if i < 0 {
		sign = 1
		i = -i
	}

	if err := w.writeByte(sign); err != nil {
		return fmt.Errorf("unable to write sign byte: %w", err)
	}

	for b = 0; b < int32(w.intSize); b++ {
		if err := w.writeByte(byte(i) & 0xFF); err != nil {
			return fmt.Errorf("unable to write int byte: %w", err)
		}
		i >>= 8
	}
	return nil
}

func (w *Writer) writeStr(data *string) error {

	if data != nil {
		if err := w.writeInt(int32(len([]byte(*data)))); err != nil {
			return fmt.Errorf("unable to write str length: %w", err)
		}
		if err := w.writeBuf([]byte(*data)); err != nil {
			return fmt.Errorf("unable to write string buffer: %w", err)
		}
	} else {

		if err := w.writeInt(-1); err != nil {
			return fmt.Errorf("unable to write empty string: %w", err)
		}
	}
	return nil
}
