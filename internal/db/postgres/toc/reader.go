package toc

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/rs/zerolog/log"
)

type Reader struct {
	r        io.Reader
	buf      []byte
	intSize  int
	version  int
	position int
}

func (r *Reader) prune() {
	r.buf = r.buf[:]
	r.intSize = 0
	r.version = 0
	r.position = 0
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
	return &Toc{
		Header:  header,
		Entries: entries,
	}, nil
}

func (r *Reader) readStr() (*string, error) {
	l, err := ah.readInt()
	if err != nil {
		return nil, err
	}
	if l < 0 {
		return nil, nil
	}

	buf := make([]byte, l)

	if _, err := r.r.Read(buf); err != nil {
		return nil, err
	}
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
	if _, err := r.r.Read(intBytes); err != nil {
		return 0, err
	}

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
	return res[0], err
}

func (r *Reader) readBytes(length int) ([]byte, error) {
	bytes := make([]byte, length)
	n, err := r.r.Read(bytes)
	r.position += n
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (r *Reader) scanBytes(byteVars ...*byte) error {
	bytes, err := r.readBytes(len(byteVars))
	if err != nil {
		return err
	}
	for idx, _ := range bytes {
		*byteVars[idx] = bytes[idx]
	}

	return nil
}

func (r *Reader) scanInt(byteVars ...*int32) error {

	for idx, _ := range byteVars {
		val, err := r.readInt()
		if err != nil {
			return err
		}
		*byteVars[idx] = val
	}

	return nil
}

func (r *Reader) readHeader() (*Header, error) {
	magicString, err := ah.readBytes(5)
	if err != nil {
		log.Err(err)
	}
	if string(magicString) != "PGDMP" {
		return errors.New("did not find magic string in srcFile handler")
	}
	if err = ah.scanBytes(&ah.VersionMajor, &ah.VersionMinor); err != nil {
		return fmt.Errorf("unable to scan major and minor version data: %w", err)
	}

	if ah.VersionMajor > 1 || (ah.VersionMajor == 1 && ah.VersionMinor > 0) {
		if err = ah.scanBytes(&ah.VersionRev); err != nil {
			return fmt.Errorf("unable to scan rev version data: %w", err)
		}
	}

	ah.Version = MakeArchiveVersion(ah.VersionMajor, ah.VersionMinor, ah.VersionRev)

	if ah.Version < BackupVersions["1.0"] || ah.Version > BackupVersions[MaxVersion] {
		return fmt.Errorf("unsupported archive version %d.%d", ah.VersionMajor, ah.VersionMinor)
	}

	// TODO: You should perform int value check if it is not suitable for current int size
	// 	you have to write warnings
	intSize, err := ah.readByte()
	if err != nil {
		return fmt.Errorf("cannot read intSize value: %w", err)
	}
	ah.IntSize = uint32(intSize)
	if intSize > 32 {
		return fmt.Errorf("sanity check on integer size %d failed", ah.IntSize)
	}

	if ah.Version >= BackupVersions["1.7"] {
		offSize, err := ah.readByte()
		if err != nil {
			return fmt.Errorf("cannot read intSize value: %w", err)
		}
		ah.OffSize = uint32(offSize)
	} else {
		ah.OffSize = ah.IntSize
	}

	if err := ah.scanBytes(&ah.Format); err != nil {
		return fmt.Errorf("unable to scan bytes from TOC srcFile: %w", err)
	}
	// I don't know why, but when pg_dump creates dump as -Fc it has Tar format assigned
	if ArchTar != ah.Format {
		return fmt.Errorf("unsupported format \"%s\" suports only directory", BackupFormats[ah.Format])
	}

	// TODO: Warning this part is distinguish from the 15 pg version. Take a look on it once pg16 will be released
	if ah.Version >= BackupVersions["1.15"] {
		algorithm, err := ah.readByte()
		if err != nil {
			return fmt.Errorf("unable to scan CompressionSpec.Algorithm: %w", err)
		}
		ah.CompressionSpec.Algorithm = int32(algorithm)
	} else if ah.Version >= BackupVersions["1.2"] {
		if ah.Version < BackupVersions["1.4"] {
			level, err := ah.readByte()
			if err != nil {
				return fmt.Errorf("unable to scan CompressionSpec.Level: %w", err)
			}
			ah.CompressionSpec.Level = int32(level)
		} else {
			if err = ah.scanInt(&ah.CompressionSpec.Level); err != nil {
				return fmt.Errorf("unable to scan CompressionSpec.Level: %w", err)
			}
		}
		if ah.CompressionSpec.Level != 0 {
			ah.CompressionSpec.Algorithm = PgCompressionGzip
		}
	} else {
		ah.CompressionSpec.Level = PgCompressionGzip

	}

	// TODO: Ensure we support compression specification

	if ah.Version >= BackupVersions["1.4"] {
		var tmSec, tmMin, tmHour, tmDay, tmMon, tmYear, tmIsDst int32
		if err = ah.scanInt(&tmSec, &tmMin, &tmHour, &tmDay, &tmMon, &tmYear, &tmIsDst); err != nil {
			return fmt.Errorf("cannot scan backup date: %w", err)
		}
		ah.CrtmDateTime = Crtm{
			TmSec:   tmSec,
			TmMin:   tmMin,
			TmHour:  tmHour,
			TmMday:  tmDay,
			TmMon:   tmMon,
			TmYear:  tmYear,
			TmIsDst: tmIsDst,
		}
	}

	if ah.Version >= BackupVersions["1.4"] {
		archDbName, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot read archdbname: %w", err)
		}
		ah.ArchDbName = archDbName
	}

	if ah.Version >= BackupVersions["1.10"] {
		archiveRemoteVersion, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot rad archiveRemoteVersion: %w", err)
		}
		ah.ArchiveRemoteVersion = archiveRemoteVersion

		archiveDumpVersion, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot read archiveDumpVersion: %w", err)
		}
		ah.ArchiveDumpVersion = archiveDumpVersion
	}

	return nil
}

func (r *Reader) readEntries() ([]*Entry, error) {

	if err := ah.scanInt(&ah.TocCount); err != nil {
		return fmt.Errorf("cannot scan tocCount: %w", err)
	}
	ah.MaxDumpId = 0

	tocList := make([]*Entry, 0)

	for i := int32(0); i < ah.TocCount; i++ {
		te := Entry{}
		if err := ah.scanInt(&te.DumpId); err != nil {
			return fmt.Errorf("cannot scan tocCount: %w", err)
		}

		if ah.MaxDumpId < te.DumpId {
			ah.MaxDumpId = te.DumpId
		}

		if te.DumpId <= 0 {
			return fmt.Errorf("entry ID %d out of range perhaps a corrupt TOC", te.DumpId)
		}

		if err := ah.scanInt(&te.HadDumper); err != nil {
			return fmt.Errorf("cannot scan hadDumer data: %w", err)
		}

		if ah.Version >= BackupVersions["1.8"] {
			tmp, err := ah.readStr()
			if err != nil {
				return fmt.Errorf("cannot read CatalogId: %w", err)
			}
			if tmp == nil {
				return errors.New("unexpected nil pointer")
			}
			tableOid, err := strconv.ParseUint(*tmp, 10, 32)
			if err != nil {
				return fmt.Errorf("cannot cast str to uint32: %w", err)
			}
			te.CatalogId.TableOid = Oid(tableOid)
		} else {
			te.CatalogId.TableOid = InvalidOid
		}
		tmp, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot read CatalogId: %w", err)
		}
		if tmp == nil {
			return errors.New("unexpected nil pointer")
		}
		oid, err := strconv.ParseUint(*tmp, 10, 32)
		if err != nil {
			return fmt.Errorf("cannot cast str to uint32: %w", err)
		}
		te.CatalogId.Oid = Oid(oid)

		tag, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot read Tag: %w", err)
		}
		te.Tag = tag

		desc, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot read Desc: %w", err)
		}
		te.Desc = desc

		if ah.Version >= BackupVersions["1.11"] {
			if err = ah.scanInt(&te.Section); err != nil {
				return fmt.Errorf("cannot Section: %w", err)
			}
		} else {
			if slices.Contains([]string{"COMMENT", "ACL", "ACL LANGUAGE"}, *te.Desc) {
				te.Section = SectionNone
			} else if slices.Contains([]string{"TABLE DATA", "BLOBS", "BLOB COMMENTS"}, *te.Desc) {
				te.Section = SectionData
			} else if slices.Contains([]string{
				"CONSTRAINT", "CHECK CONSTRAINT", "FK CONSTRAINT", "INDEX", "RULE", "TRIGGER",
			}, *te.Desc) {
				te.Section = SectionPostData
			} else {
				te.Section = SectionPreData
			}
		}

		defn, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot read Defn: %w", err)
		}
		te.Defn = defn

		dropStmt, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot read DropStmt: %w", err)
		}
		te.DropStmt = dropStmt

		if ah.Version >= BackupVersions["1.3"] {
			copyStmt, err := ah.readStr()
			if err != nil {
				return fmt.Errorf("cannot read Defn: %w", err)
			}
			te.CopyStmt = copyStmt
		}

		if ah.Version >= BackupVersions["1.6"] {
			namespace, err := ah.readStr()
			if err != nil {
				return fmt.Errorf("cannot read Namespace: %w", err)
			}
			te.Namespace = namespace
		}

		if ah.Version >= BackupVersions["1.10"] {
			tablespace, err := ah.readStr()
			if err != nil {
				return fmt.Errorf("cannot read Tablespace: %w", err)
			}
			te.Tablespace = tablespace
		}

		if ah.Version >= BackupVersions["1.14"] {
			tableam, err := ah.readStr()
			if err != nil {
				return fmt.Errorf("cannot read Tableam: %w", err)
			}
			te.Tableam = tableam
		}

		owner, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot read Tablespace: %w", err)
		}
		te.Owner = owner

		isSupported := true
		if ah.Version < BackupVersions["1.9"] {
			isSupported = false
		} else {
			tmp, err = ah.readStr()
			if err != nil {
				return fmt.Errorf("cannot read CatalogId: %w", err)
			}
			if tmp == nil {
				return errors.New("unexpected nil pointer")
			}
			if *tmp == "true" {
				isSupported = false
			}
		}

		if !isSupported {
			log.Warn().Msg("restoring tables WITH OIDS is not supported anymore")
		}

		/* Read TOC entry Dependencies */
		if ah.Version >= BackupVersions["1.5"] {
			te.Dependencies = make([]int32, 0, 10)
			for {
				tmp, err = ah.readStr()
				if err != nil {
					return fmt.Errorf("cannot read CatalogId: %w", err)
				}
				if tmp == nil {
					break
				}

				val, err := strconv.ParseInt(*tmp, 10, 32)
				if err != nil {
					return fmt.Errorf("unable to parse dependency int32 value: %w", err)
				}

				te.Dependencies = append(te.Dependencies, int32(val))
			}
			te.NDeps = int32(len(te.Dependencies))

		} else {
			te.Dependencies = nil
			te.NDeps = 0
		}
		te.DataLength = 0

		// TODO: Here we are executing ReadExtraTocPtr - and it depends on the objects or even versiob
		//		 it may rise an error later.
		fileName, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot read an additional FileName data: %w", err)
		}
		te.FileName = fileName
		tocList = append(tocList, &te)

	}
	ah.tocList = tocList
	return nil
}
