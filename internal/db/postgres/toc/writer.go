package toc

import (
	"errors"
	"fmt"
	"io"
	"strconv"
)

type Writer struct {
	w io.Writer
}

func Write(toc *Toc, w io.Writer) error {
	if toc.Entries == nil {
		return errors.New("entries are nil")
	}
	if toc.Header == nil {
		return errors.New("header is nil")
	}
	if err := writeHeader(toc.Header, w); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	if err := writeEntries(toc.Entries, w); err != nil {
		return fmt.Errorf("error writing entries: %w", err)
	}
	return nil
}

func writeHeader(header *Header, w io.Writer) error {
	if err := ah.writeBuf([]byte("PGDMP")); err != nil {
		return fmt.Errorf("cannot write magic str: %w", err)
	}
	if err := ah.writeByte(ah.VersionMajor); err != nil {
		return fmt.Errorf("cannot write versionMajor: %w", err)
	}

	if err := ah.writeByte(ah.VersionMinor); err != nil {
		return fmt.Errorf("cannot write versionMinor: %w", err)
	}

	if ah.VersionMajor > 1 || (ah.VersionMajor == 1 && ah.VersionMinor > 0) {
		if err := ah.writeByte(ah.VersionRev); err != nil {
			return fmt.Errorf("cannot write versionRev: %w", err)
		}
	}

	if err := ah.writeByte(byte(ah.IntSize)); err != nil {
		return fmt.Errorf("cannot write intSize: %w", err)
	}

	if ah.Version >= BackupVersions["1.7"] {
		if err := ah.writeByte(byte(ah.OffSize)); err != nil {
			return fmt.Errorf("cannot write offSize: %w", err)
		}
	}

	// TODO: Fixme - I've changed hardcoded ArchTar to ah.Format - it may bring an error
	if err := ah.writeByte(ah.Format); err != nil {
		return fmt.Errorf("cannot write format: %w", err)
	}

	// TODO: discover about compressionNotSet - how it is determining in the C code
	var compressionNotSet int32 = -1
	if ah.Version >= BackupVersions["1.15"] {
		if err := ah.writeInt(compressionNotSet); err != nil {
			return fmt.Errorf("cannot write CompressionSpec.Algorithm: %w", err)
		}
	} else if ah.Version >= BackupVersions["1.2"] {
		if ah.Version < BackupVersions["1.4"] {
			if err := ah.writeByte(byte(ah.CompressionSpec.Level)); err != nil {
				return fmt.Errorf("cannot write CompressionSpec.Algorithm: %w", err)
			}
		} else {
			if err := ah.writeInt(ah.CompressionSpec.Level); err != nil {
				return fmt.Errorf("cannot write CompressionSpec.Algorithm: %w", err)
			}
		}
	}

	if ah.Version >= BackupVersions["1.4"] {
		if err := ah.writeInt(ah.CrtmDateTime.TmSec); err != nil {
			return fmt.Errorf("cannot write TmSec: %w", err)
		}
		if err := ah.writeInt(ah.CrtmDateTime.TmMin); err != nil {
			return fmt.Errorf("cannot write TmMin: %w", err)
		}
		if err := ah.writeInt(ah.CrtmDateTime.TmHour); err != nil {
			return fmt.Errorf("cannot write TmHour: %w", err)
		}
		if err := ah.writeInt(ah.CrtmDateTime.TmMday); err != nil {
			return fmt.Errorf("cannot write TmMday: %w", err)
		}
		if err := ah.writeInt(ah.CrtmDateTime.TmMon); err != nil {
			return fmt.Errorf("cannot write TmMon: %w", err)
		}
		if err := ah.writeInt(ah.CrtmDateTime.TmYear); err != nil {
			return fmt.Errorf("cannot write TmYear: %w", err)
		}
		if err := ah.writeInt(ah.CrtmDateTime.TmIsDst); err != nil {
			return fmt.Errorf("cannot write TmIsDst: %w", err)
		}
	}

	if ah.Version >= BackupVersions["1.4"] {
		if err := ah.writeStr(ah.ArchDbName); err != nil {
			return fmt.Errorf("cannot write archDbName: %w", err)
		}
	}

	if ah.Version >= BackupVersions["1.10"] {
		if err := ah.writeStr(ah.ArchiveRemoteVersion); err != nil {
			return fmt.Errorf("cannot write archiveRemoteVersion: %w", err)
		}
		if err := ah.writeStr(ah.ArchiveDumpVersion); err != nil {
			return fmt.Errorf("cannot write archiveDumpVersion: %w", err)
		}
	}

	return nil
}

func writeEntries(entries []*Entry, w io.Writer) error {
	var tocCount = int32(len(ah.tocList))

	if err := ah.writeInt(tocCount); err != nil {
		return fmt.Errorf("cannot write tocCount: %w", err)
	}

	for _, te := range ah.tocList {
		if err := ah.writeInt(te.DumpId); err != nil {
			panic(fmt.Sprintf("unable to write DumpId: %s", err))
		}

		if err := ah.writeInt(te.HadDumper); err != nil {
			panic(fmt.Sprintf("unable to write DataDumper: %s", err))
		}

		if ah.Version >= BackupVersions["1.8"] {
			oidStr := strconv.FormatUint(uint64(te.CatalogId.TableOid), 10)
			if err := ah.writeStr(&oidStr); err != nil {
				panic(fmt.Sprintf("unable to write TableOid: %s", err))
			}
		}

		oidStr := strconv.FormatUint(uint64(te.CatalogId.Oid), 10)
		if err := ah.writeStr(&oidStr); err != nil {
			panic(fmt.Sprintf("unable to write Oid: %s", err))
		}

		if err := ah.writeStr(te.Tag); err != nil {
			panic(fmt.Sprintf("unable to write Tag: %s", err))
		}
		if err := ah.writeStr(te.Desc); err != nil {
			panic(fmt.Sprintf("unable to write Desc: %s", err))
		}

		if ah.Version >= BackupVersions["1.11"] {
			if err := ah.writeInt(te.Section); err != nil {
				panic(fmt.Sprintf("unable to write Section: %s", err))
			}
		}

		if err := ah.writeStr(te.Defn); err != nil {
			panic(fmt.Sprintf("unable to write Defn: %s", err))
		}
		if err := ah.writeStr(te.DropStmt); err != nil {
			panic(fmt.Sprintf("unable to write DropStmt: %s", err))
		}

		if ah.Version >= BackupVersions["1.3"] {
			if err := ah.writeStr(te.CopyStmt); err != nil {
				panic(fmt.Sprintf("unable to write CopyStmt: %s", err))
			}
		}

		if ah.Version >= BackupVersions["1.6"] {
			if err := ah.writeStr(te.Namespace); err != nil {
				panic(fmt.Sprintf("unable to write Namespace: %s", err))
			}
		}

		if ah.Version >= BackupVersions["1.10"] {
			if err := ah.writeStr(te.Tablespace); err != nil {
				panic(fmt.Sprintf("unable to write Tablespace: %s", err))
			}
		}

		if ah.Version >= BackupVersions["1.14"] {
			if err := ah.writeStr(te.Tableam); err != nil {
				panic(fmt.Sprintf("unable to write Tableam: %s", err))
			}
		}

		if err := ah.writeStr(te.Owner); err != nil {
			panic(fmt.Sprintf("unable ro write Owner: %s", err))
		}

		if ah.Version >= BackupVersions["1.9"] {
			tableOidRestoring := "false"
			if err := ah.writeStr(&tableOidRestoring); err != nil {
				panic(fmt.Sprintf("unable to write \"false\" value: %s", err))
			}
		}

		if ah.Version >= BackupVersions["1.5"] {
			for _, d := range te.Dependencies {
				depStr := strconv.FormatInt(int64(d), 10)
				if err := ah.writeStr(&depStr); err != nil {
					panic(fmt.Sprintf("unable to write entry dependency value: %s", err))
				}
			}
			/* Terminate List */
			if err := ah.writeStr(nil); err != nil {
				panic(fmt.Sprintf("unable to write entry Dependencies list terminator: %s", err))
			}
		}

		// TODO: Ensure te.FileName is required for all versions
		// WriteExtraTocPtr - write filename here
		if err := ah.writeStr(te.FileName); err != nil {
			panic(fmt.Sprintf("unable to write FileName: %s", err))
		}

	}

	return nil
}

func (ah *ArchiveHandle) writeBuf(buf []byte) error {
	_, err := ah.destFile.Write(buf)
	//ah.WrittenBytes += int64(n)
	if err != nil {
		return err
	}

	return nil
}

func (ah *ArchiveHandle) writeByte(data byte) error {
	err := ah.writeBuf([]byte{data})
	if err != nil {
		return fmt.Errorf("cannot write byte data: %w", err)
	}
	return nil
}

func (ah *ArchiveHandle) writeInt(i int32) error {
	var b int32
	var sign byte

	if i < 0 {
		sign = 1
		i = -i
	}

	if err := ah.writeByte(sign); err != nil {
		return fmt.Errorf("unable to write sign byte: %w", err)
	}

	for b = 0; b < int32(ah.IntSize); b++ {
		if err := ah.writeByte(byte(i) & 0xFF); err != nil {
			return fmt.Errorf("unable to write int byte: %w", err)
		}
		i >>= 8
	}
	return nil
}

func (ah *ArchiveHandle) writeStr(data *string) error {

	if data != nil {
		if err := ah.writeInt(int32(len([]byte(*data)))); err != nil {
			return fmt.Errorf("unable to write str length: %w", err)
		}
		if err := ah.writeBuf([]byte(*data)); err != nil {
			return fmt.Errorf("unable to write string buffer: %w", err)
		}
	} else {

		if err := ah.writeInt(-1); err != nil {
			return fmt.Errorf("unable to write empty string: %w", err)
		}
	}
	return nil
}
