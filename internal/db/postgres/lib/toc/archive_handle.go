package toc

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
)

const (
	InvalidOid = 0
)

type ArchiveHandle struct {
	Header
	srcFile      io.Reader
	destFile     io.Writer
	WrittenBytes int64
	ReadBytes    int64
	tocList      []*Entry
	dumpId       int32
}

func NewArchiveHandle(srcFile io.Reader, destFile io.ReadWriteSeeker) *ArchiveHandle {
	return &ArchiveHandle{
		srcFile:  srcFile,
		destFile: destFile,
		Header: Header{
			Format: ArchTar,
		},
	}
}

func (ah *ArchiveHandle) readStr() (*string, error) {
	l, err := ah.readInt()
	if err != nil {
		return nil, err
	}
	if l < 0 {
		return nil, nil
	}

	buf := make([]byte, l)

	if _, err := ah.srcFile.Read(buf); err != nil {
		return nil, err
	}
	strVal := string(buf)
	return &strVal, nil
}

func (ah *ArchiveHandle) readInt() (int32, error) {
	var sign byte = 0
	var err error
	var res, bitShift int32

	if ah.IntSize != 4 {
		return 0, errors.New("unsupported int32 size")
	}

	if ah.Version == 0 {
		return 0, errors.New("version cannot be 0")
	}

	if ah.Version > BackupVersions["1.0"] {
		sign, err = ah.readByte()
		if err != nil {
			return 0, fmt.Errorf("cannot read srcFile byte: %s", err)
		}
	}

	intBytes := make([]byte, ah.IntSize)
	if _, err := ah.srcFile.Read(intBytes); err != nil {
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

func (ah *ArchiveHandle) readByte() (byte, error) {
	res, err := ah.readBytes(1)
	return res[0], err
}

func (ah *ArchiveHandle) readBytes(length int) ([]byte, error) {
	bytes := make([]byte, length)
	n, err := ah.srcFile.Read(bytes)
	ah.ReadBytes += int64(n)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (ah *ArchiveHandle) scanBytes(byteVars ...*byte) error {
	bytes, err := ah.readBytes(len(byteVars))
	if err != nil {
		return err
	}
	for idx, _ := range bytes {
		*byteVars[idx] = bytes[idx]
	}

	return nil
}

func (ah *ArchiveHandle) scanInt(byteVars ...*int32) error {

	for idx, _ := range byteVars {
		val, err := ah.readInt()
		if err != nil {
			return err
		}
		*byteVars[idx] = val
	}

	return nil
}

func (ah *ArchiveHandle) readHead() error {
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

func (ah *ArchiveHandle) readToc() error {

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

func (ah *ArchiveHandle) GetEntries() []*Entry {
	return ah.tocList
}

func (ah *ArchiveHandle) SetEntries(entries []*Entry) {
	ah.tocList = entries
}

func (ah *ArchiveHandle) writeHeadV2() error {
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

func (ah *ArchiveHandle) writeTocV2() error {
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

func (ah *ArchiveHandle) writeHead() error {

	if err := ah.writeBuf([]byte("PGDMP")); err != nil {
		return fmt.Errorf("cannot write magic str: %w", err)
	}

	if err := ah.writeByte(ah.VersionMajor); err != nil {
		return fmt.Errorf("cannot write versionMajor: %w", err)
	}

	if err := ah.writeByte(ah.VersionMinor); err != nil {
		return fmt.Errorf("cannot write versionMinor: %w", err)
	}

	if err := ah.writeByte(ah.VersionRev); err != nil {
		return fmt.Errorf("cannot write versionRev: %w", err)
	}

	if err := ah.writeByte(byte(ah.IntSize)); err != nil {
		return fmt.Errorf("cannot write intSize: %w", err)
	}

	if err := ah.writeByte(byte(ah.OffSize)); err != nil {
		return fmt.Errorf("cannot write offSize: %w", err)
	}

	if err := ah.writeByte(ArchTar); err != nil {
		return fmt.Errorf("cannot write format: %w", err)
	}

	var compressionNotSet int32 = -1
	if err := ah.writeInt(compressionNotSet); err != nil {
		return fmt.Errorf("cannot write CompressionSpec.Algorithm: %w", err)
	}

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
	//connectionString := ""
	if err := ah.writeStr(ah.ArchDbName); err != nil {
		return fmt.Errorf("cannot write archDbName: %w", err)
	}
	if err := ah.writeStr(ah.ArchiveRemoteVersion); err != nil {
		return fmt.Errorf("cannot write archiveRemoteVersion: %w", err)
	}
	if err := ah.writeStr(ah.ArchiveDumpVersion); err != nil {
		return fmt.Errorf("cannot write archiveDumpVersion: %w", err)
	}

	return nil
}

func (ah *ArchiveHandle) writeToc() error {
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

		oidStr := strconv.FormatUint(uint64(te.CatalogId.TableOid), 10)
		if err := ah.writeStr(&oidStr); err != nil {
			panic(fmt.Sprintf("unable to write TableOid: %s", err))
		}

		oidStr = strconv.FormatUint(uint64(te.CatalogId.Oid), 10)
		if err := ah.writeStr(&oidStr); err != nil {
			panic(fmt.Sprintf("unable to write Oid: %s", err))
		}

		if err := ah.writeStr(te.Tag); err != nil {
			panic(fmt.Sprintf("unable to write Tag: %s", err))
		}
		if err := ah.writeStr(te.Desc); err != nil {
			panic(fmt.Sprintf("unable to write Desc: %s", err))
		}
		if err := ah.writeInt(te.Section); err != nil {
			panic(fmt.Sprintf("unable to write Section: %s", err))
		}
		if err := ah.writeStr(te.Defn); err != nil {
			panic(fmt.Sprintf("unable to write Defn: %s", err))
		}
		if err := ah.writeStr(te.DropStmt); err != nil {
			panic(fmt.Sprintf("unable to write DropStmt: %s", err))
		}
		if err := ah.writeStr(te.CopyStmt); err != nil {
			panic(fmt.Sprintf("unable to write CopyStmt: %s", err))
		}
		if err := ah.writeStr(te.Namespace); err != nil {
			panic(fmt.Sprintf("unable to write Namespace: %s", err))
		}
		if err := ah.writeStr(te.Tablespace); err != nil {
			panic(fmt.Sprintf("unable to write Tablespace: %s", err))
		}
		if err := ah.writeStr(te.Tableam); err != nil {
			panic(fmt.Sprintf("unable to write Tableam: %s", err))
		}
		if err := ah.writeStr(te.Owner); err != nil {
			panic(fmt.Sprintf("unable ro write Owner: %s", err))
		}
		// TODO: What is that value?
		someFalseValue := "false"
		if err := ah.writeStr(&someFalseValue); err != nil {
			panic(fmt.Sprintf("unable to write \"false\" value: %s", err))
		}

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

		// WriteExtraTocPtr - write filename here
		if err := ah.writeStr(te.FileName); err != nil {
			panic(fmt.Sprintf("unable to write FileName: %s", err))
		}

	}

	return nil
}

func (ah *ArchiveHandle) writeBuf(buf []byte) error {
	n, err := ah.destFile.Write(buf)
	ah.WrittenBytes += int64(n)
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

func ReadFile(reader io.Reader) (*ArchiveHandle, error) {
	ah := NewArchiveHandle(reader, nil)

	if err := ah.readHead(); err != nil {
		return nil, err
	}

	if err := ah.readToc(); err != nil {
		return nil, err
	}
	return ah, nil
}

func WriteFile(ah *ArchiveHandle, writer io.Writer) error {
	ah.destFile = writer
	ah.WrittenBytes = 0
	if err := ah.writeHeadV2(); err != nil {
		return err
	}

	if err := ah.writeTocV2(); err != nil {
		return err
	}
	return nil
}
