package toc

import (
	"container/ring"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"strconv"
	"time"
)

const (
	InvalidOid = 0
)

type crtm struct {
	TmSec   int32
	TmMin   int32
	TmHour  int32
	TmMday  int32
	TmMon   int32
	TmYear  int32
	TmIsDst int32
}

type ArchiveHandle struct {
	srcFile         io.ReadSeeker
	destFile        io.WriteSeeker
	tocWrittenBytes int
	//Archive public  /* Public part of archive */
	version              int /* Version of file */
	versionMajor         byte
	versionMinor         byte
	versionRev           byte
	archiveRemoteVersion *string /* When reading an archive, the
	 * version of the dumped DB */
	archiveDumpVersion *string /* When reading an archive, the version of
	 * the dumper */
	intSize uint32 /* Size of an integer in the archive */
	offSize uint32 /* Size of a file offset in the archive */

	format          byte /* Archive format */
	crtmDateTime    crtm
	createDate      time.Time /* Date archive created */
	archDbName      *string
	toc             *ring.Ring /* Header of circular list of TOC entries */
	tocHead         *ring.Ring /* Header of circular list of TOC entries */
	tocList         []Entry
	tocCount        int32 /* Number of TOC entries */
	maxDumpId       int32 /* largest DumpId among all TOC entries */
	dumpId          int32
	CompressionSpec CompressionSpecification /* Requested specification */
	compression     int32
}

func NewArchiveHandle(srcFile io.ReadWriteSeeker, destFile io.ReadWriteSeeker) *ArchiveHandle {
	return &ArchiveHandle{
		srcFile:  srcFile,
		destFile: destFile,
		format:   ArchTar,
		toc:      nil,
	}
}

func (ah *ArchiveHandle) readHead() error {
	var major, minor, rev byte
	var format byte
	magicString, err := ah.readBytes(5)
	if err != nil {
		log.Err(err)
	}
	if string(magicString) != "PGDMP" {
		return errors.New("did not find magic string in srcFile handler")
	}
	if err = ah.scanBytes(&major, &minor); err != nil {
		return fmt.Errorf("unable to scan major and minor version data: %w", err)
	}
	ah.versionMajor = major
	ah.versionMinor = minor

	if major > 1 || (major == 1 && minor > 0) {
		if err = ah.scanBytes(&rev); err != nil {
			return fmt.Errorf("unable to scan rev version data: %w", err)
		}
		ah.versionRev = rev
	}

	ah.version = MakeArchiveVersion(major, minor, rev)

	if ah.version < BackupVersions["1.0"] || ah.version > BackupVersions["1.15"] {
		return fmt.Errorf("unsupported archive version %d.%d", major, minor)
	}

	intSize, err := ah.readByte()
	if err != nil {
		return fmt.Errorf("cannot read intSize value: %w", err)
	}
	ah.intSize = uint32(intSize)

	if ah.version >= BackupVersions["1.7"] {
		offSize, err := ah.readByte()
		if err != nil {
			return fmt.Errorf("cannot read intSize value: %w", err)
		}
		ah.offSize = uint32(offSize)
	} else {
		ah.offSize = ah.intSize
	}

	if err := ah.scanBytes(&format); err != nil {
		return fmt.Errorf("unable to scan bytes from TOC srcFile: %w", err)
	}
	if ArchTar != format {
		return fmt.Errorf("unsupported format \"%s\" suports only directory", BackupFormats[format])
	}

	if ah.version >= BackupVersions["1.15"] {
		algorithm, err := ah.readByte()
		if err != nil {
			return fmt.Errorf("unable to scan CompressionSpec.Algorithm: %w", err)
		}
		ah.CompressionSpec.Algorithm = int32(algorithm)
		ah.compression = int32(algorithm)
	} else if ah.version >= BackupVersions["1.2"] {
		if ah.version < BackupVersions["1.4"] {
			level, err := ah.readByte()
			if err != nil {
				return fmt.Errorf("unable to scan CompressionSpec.Level: %w", err)
			}
			ah.CompressionSpec.Level = int32(level)
		} else {
			if err = ah.scanInt(&ah.CompressionSpec.Level); err != nil {
				return fmt.Errorf("unable to scan CompressionSpec.Level: %w", err)
			}

			if ah.CompressionSpec.Level != 0 {
				ah.CompressionSpec.Algorithm = PgCompressionGzip
			}
		}
	} else {
		ah.CompressionSpec.Level = PgCompressionGzip

	}

	// TODO: Ensure we support compression specification

	if ah.version >= BackupVersions["1.4"] {
		var tmSec, tmMin, tmHour, tmDay, tmMon, tmYear, tmIsDst int32
		if err = ah.scanInt(&tmSec, &tmMin, &tmHour, &tmDay, &tmMon, &tmYear, &tmIsDst); err != nil {
			return fmt.Errorf("cannot scan backup date: %w", err)
		}
		ah.crtmDateTime = crtm{
			TmSec:   tmSec,
			TmMin:   tmMin,
			TmHour:  tmHour,
			TmMday:  tmDay,
			TmMon:   tmMon,
			TmYear:  tmYear,
			TmIsDst: tmIsDst,
		}

		loc, err := time.LoadLocation("UTC")
		if err != nil {
			return err
		}
		ah.createDate = time.Date(int(1900+tmYear), time.Month(tmMon), int(tmDay), int(tmHour), int(tmMin), int(tmSec), 0, loc)
	}

	if ah.version >= BackupVersions["1.4"] {
		archDbName, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot read archdbname: %w", err)
		}
		ah.archDbName = archDbName
	}

	if ah.version >= BackupVersions["1.10"] {
		archiveRemoteVersion, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot rad archiveRemoteVersion: %w", err)
		}
		ah.archiveRemoteVersion = archiveRemoteVersion

		archiveDumpVersion, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot read archiveDumpVersion: %w", err)
		}
		ah.archiveDumpVersion = archiveDumpVersion
	}

	return nil
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

func (ah *ArchiveHandle) getCurFilePos() int64 {
	pos, _ := ah.destFile.Seek(0, io.SeekCurrent)
	return pos
}

func (ah *ArchiveHandle) readInt() (int32, error) {
	var sign byte = 0
	var err error
	var res, bitShift int32

	if ah.intSize != 4 {
		return 0, errors.New("unsupported int32 size")
	}

	if ah.version == 0 {
		return 0, errors.New("version cannot be 0")
	}

	if ah.version > BackupVersions["1.0"] {
		sign, err = ah.readByte()
		if err != nil {
			return 0, fmt.Errorf("cannot read srcFile byte: %s", err)
		}
	}

	intBytes := make([]byte, ah.intSize)
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
	if _, err := ah.srcFile.Read(bytes); err != nil {
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

func (ah *ArchiveHandle) readToc() error {

	if err := ah.scanInt(&ah.tocCount); err != nil {
		return fmt.Errorf("cannot scan tocCount: %w", err)
	}
	ah.maxDumpId = 0

	tocList := make([]Entry, 0)

	for i := int32(0); i < ah.tocCount; i++ {
		te := Entry{}
		if err := ah.scanInt(&te.DumpId); err != nil {
			return fmt.Errorf("cannot scan tocCount: %w", err)
		}

		if te.DumpId <= 0 {
			return fmt.Errorf("entry ID %d out of range perhaps a corrupt TOC", te.DumpId)
		}
		if te.DumpId > ah.maxDumpId {
			ah.maxDumpId = te.DumpId
		}

		if err := ah.scanInt(&te.HadDumper); err != nil {
			return fmt.Errorf("cannot scan hadDumer data: %w", err)
		}

		if ah.version >= BackupVersions["1.8"] {
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
			te.CatalogId.tableOid = Oid(tableOid)
		} else {
			te.CatalogId.tableOid = InvalidOid
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
		te.CatalogId.oid = Oid(oid)

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

		if ah.version >= BackupVersions["1.11"] {
			if err = ah.scanInt(&te.Section); err != nil {
				return fmt.Errorf("cannot Section: %w", err)
			}
		} else {
			return errors.New("unsupported version")
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

		if ah.version >= BackupVersions["1.3"] {
			copyStmt, err := ah.readStr()
			if err != nil {
				return fmt.Errorf("cannot read Defn: %w", err)
			}
			te.CopyStmt = copyStmt
		}

		if ah.version >= BackupVersions["1.6"] {
			namespace, err := ah.readStr()
			if err != nil {
				return fmt.Errorf("cannot read Namespace: %w", err)
			}
			te.Namespace = namespace
		}

		if ah.version >= BackupVersions["1.10"] {
			tablespace, err := ah.readStr()
			if err != nil {
				return fmt.Errorf("cannot read Tablespace: %w", err)
			}
			te.Tablespace = tablespace
		}

		if ah.version >= BackupVersions["1.14"] {
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
		if ah.version < BackupVersions["1.9"] {
			isSupported = false
		} else {
			tmp, err := ah.readStr()
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
			//return errors.New("unsupported version")
		}

		/* Read TOC entry Dependencies */
		if ah.version >= BackupVersions["1.5"] {
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

		fileName, err := ah.readStr()
		if err != nil {
			return fmt.Errorf("cannot additional data FileName: %w", err)
		}
		te.FileName = fileName

		// 		/* link completed entry into TOC circular list */
		//		te->prev = AH->toc->prev;
		//		AH->toc->prev->next = te;
		//		AH->toc->prev = te;
		//		te->next = AH->toc;

		tocList = append(tocList, te)

		r := ring.New(1)
		r.Value = te
		if ah.toc == nil {
			ah.toc = r
			ah.tocHead = r
		} else {
			ah.toc.Link(r)
		}

	}
	ah.tocList = tocList
	return nil
}

func (ah *ArchiveHandle) GetEntries() []Entry {
	return ah.tocList
}

func (ah *ArchiveHandle) SetEntries(entries []Entry) {
	ah.tocList = entries
}

func (ah *ArchiveHandle) writeHead() error {

	if err := ah.writeBuf([]byte("PGDMP")); err != nil {
		return fmt.Errorf("cannot write magic str: %w", err)
	}

	if err := ah.writeByte(ah.versionMajor); err != nil {
		return fmt.Errorf("cannot write versionMajor: %w", err)
	}

	if err := ah.writeByte(ah.versionMinor); err != nil {
		return fmt.Errorf("cannot write versionMinor: %w", err)
	}

	if err := ah.writeByte(ah.versionRev); err != nil {
		return fmt.Errorf("cannot write versionRev: %w", err)
	}

	if err := ah.writeByte(byte(ah.intSize)); err != nil {
		return fmt.Errorf("cannot write intSize: %w", err)
	}

	if err := ah.writeByte(byte(ah.offSize)); err != nil {
		return fmt.Errorf("cannot write offSize: %w", err)
	}

	if err := ah.writeByte(ArchTar); err != nil {
		return fmt.Errorf("cannot write format: %w", err)
	}

	var compressionNotSet int32 = -1
	if err := ah.writeInt(compressionNotSet); err != nil {
		return fmt.Errorf("cannot write CompressionSpec.Algorithm: %w", err)
	}

	if err := ah.writeInt(ah.crtmDateTime.TmSec); err != nil {
		return fmt.Errorf("cannot write TmSec: %w", err)
	}

	if err := ah.writeInt(ah.crtmDateTime.TmMin); err != nil {
		return fmt.Errorf("cannot write TmMin: %w", err)
	}
	if err := ah.writeInt(ah.crtmDateTime.TmHour); err != nil {
		return fmt.Errorf("cannot write TmHour: %w", err)
	}
	if err := ah.writeInt(ah.crtmDateTime.TmMday); err != nil {
		return fmt.Errorf("cannot write TmMday: %w", err)
	}
	if err := ah.writeInt(ah.crtmDateTime.TmMon); err != nil {
		return fmt.Errorf("cannot write TmMon: %w", err)
	}
	if err := ah.writeInt(ah.crtmDateTime.TmYear); err != nil {
		return fmt.Errorf("cannot write TmYear: %w", err)
	}
	if err := ah.writeInt(ah.crtmDateTime.TmIsDst); err != nil {
		return fmt.Errorf("cannot write TmIsDst: %w", err)
	}
	//connectionString := ""
	if err := ah.writeStr(ah.archDbName); err != nil {
		return fmt.Errorf("cannot write archDbName: %w", err)
	}
	if err := ah.writeStr(ah.archiveRemoteVersion); err != nil {
		return fmt.Errorf("cannot write archiveRemoteVersion: %w", err)
	}
	if err := ah.writeStr(ah.archiveDumpVersion); err != nil {
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

		oidStr := strconv.FormatUint(uint64(te.CatalogId.tableOid), 10)
		if err := ah.writeStr(&oidStr); err != nil {
			panic(fmt.Sprintf("unable to write tableOid: %s", err))
		}

		oidStr = strconv.FormatUint(uint64(te.CatalogId.oid), 10)
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
	ah.tocWrittenBytes += n
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

	for b = 0; b < int32(ah.intSize); b++ {
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

func ReadFile(filaName string) (*ArchiveHandle, error) {
	srcToc, err := os.Open(filaName)
	if err != nil {
		return nil, fmt.Errorf("unable to open TOC file: %w", err)
	}
	defer srcToc.Close()

	ah := NewArchiveHandle(srcToc, nil)

	if err := ah.readHead(); err != nil {
		return nil, err
	}

	if err := ah.readToc(); err != nil {
		return nil, err
	}
	return ah, nil
}

func WriteFile(ah *ArchiveHandle, filaName string) error {
	destToc, err := os.Create(filaName)
	if err != nil {
		return fmt.Errorf("unable to open TOC file: %w", err)
	}
	defer destToc.Close()
	ah.destFile = destToc

	if err := ah.writeHead(); err != nil {
		return err
	}

	if err := ah.writeToc(); err != nil {
		return err
	}
	return nil
}
