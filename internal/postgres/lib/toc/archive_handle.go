package toc

import (
	"container/ring"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
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
	version      int /* Version of file */
	versionMajor byte
	versionMinor byte
	versionRev   byte

	archiveRemoteVersion *string /* When reading an archive, the
	 * version of the dumped DB */
	archiveDumpVersion *string /* When reading an archive, the version of
	 * the dumper */

	intSize uint32 /* Size of an integer in the archive */
	offSize uint32 /* Size of a file offset in the archive */

	format byte /* Archive format */

	crtmDateTime crtm
	createDate   time.Time /* Date archive created */
	archDbName   *string
	//
	//ArchiveEntryPtrType ArchiveEntryPtr /* Called for each metadata object */
	//StartDataPtrType    StartDataPtr    /* Called when table data is about to be
	// * dumped */
	//WriteDataPtrType WriteDataPtr /* Called to send some table data to the
	// * archive */
	//EndDataPtrType       EndDataPtr       /* Called when table data dump is finished */
	//WriteBytePtrType     WriteBytePtr     /* Write a byte to output */
	//ReadBytePtrType      ReadBytePtr      /* Read a byte from an archive */
	//WriteBufPtrType      WriteBufPtr      /* Write a buffer of output to the archive */
	//ReadBufPtrType       ReadBufPtr       /* Read a buffer of input from the archive */
	//ClosePtrType         ClosePtr         /* Close the archive */
	//ReopenPtrType        ReopenPtr        /* Reopen the archive */
	//WriteExtraTocPtrType WriteExtraTocPtr /* Write extra TOC entry data
	// * associated with the current
	// * archive format */
	//ReadExtraTocPtrType ReadExtraTocPtr /* Read extra info associated with
	// * archive format */
	//PrintExtraTocPtrType       PrintExtraTocPtr /* Extra TOC info for format */
	//PrintTocDataPtrType        PrintTocDataPtr
	//StartLOsPtrType            StartLOsPtr
	//EndLOsPtrType              EndLOsPtr
	//StartLOPtrType             StartLOPtr
	//EndLOPtrType               EndLOPtr
	//SetupWorkerPtrType         SetupWorkerPtr
	//WorkerJobDumpPtrType       WorkerJobDumpPtr
	//WorkerJobRestorePtrType    WorkerJobRestorePtr
	//PrepParallelRestorePtrType PrepParallelRestorePtr
	//ClonePtrType               ClonePtr   /* Clone format-specific fields */
	//DeClonePtrType             DeClonePtr /* Clean up cloned fields */
	//
	//CustomOutPtrType CustomOutPtr /* Alternative script output routine */
	//
	toc       *ring.Ring /* Header of circular list of TOC entries */
	tocHead   *ring.Ring /* Header of circular list of TOC entries */
	tocList   []Entry
	tocCount  int32 /* Number of TOC entries */
	maxDumpId int32 /* largest DumpId among all TOC entries */
	dumpId    int32
	//
	///* arrays created after the TOC list is complete: */
	//_tocEntry **tocsByDumpId /* TOCs indexed by dumpId */
	//DumpId    *tableDataId   /* TABLE DATA ids, indexed by table dumpId */
	//
	//_tocEntry                 *currToc         /* Used when dumping data */
	CompressionSpec CompressionSpecification /* Requested specification */
	compression     int32
	//bool        dosync      /* data requested to be synced on sight */
	//ArchiveMode mode        /* File mode - r or w */
	//void        *formatData /* Header data specific to srcFile format */
	//
	///* these vars track state to avoid sending redundant SET commands */
	//char *currUser       /* current username, or NULL if unknown */
	//char *currSchema     /* current schema, or NULL */
	//char *currTablespace /* current tablespace, or NULL */
	//char *currTableAm    /* current table access method, or NULL */
	//
	//void          *lo_buf
	//size_t        lo_buf_used
	//size_t        lo_buf_size
	//int           noTocComments
	//ArchiverStage stage
	//ArchiverStage lastErrorStage
	//RestorePass   restorePass /* used only during parallel restore */
	//_tocEntry     *currentTE
	//_tocEntry     *lastErrorTE
}

func NewArchiveHandle(srcFile io.ReadWriteSeeker, destFile io.ReadWriteSeeker, format byte) *ArchiveHandle {
	return &ArchiveHandle{
		srcFile:  srcFile,
		destFile: destFile,
		format:   format,
		toc:      nil,
	}
}

func (ah *ArchiveHandle) ReadHead() error {
	var major, minor, rev byte
	var format byte
	magicString, err := ah.ReadBytes(5)
	if err != nil {
		log.Err(err)
	}
	if string(magicString) != "PGDMP" {
		return errors.New("did not find magic string in srcFile handler")
	}
	if err = ah.ScanBytes(&major, &minor); err != nil {
		return fmt.Errorf("unable to scan major and minor version data: %w", err)
	}
	ah.versionMajor = major
	ah.versionMinor = minor

	if major > 1 || (major == 1 && minor > 0) {
		if err = ah.ScanBytes(&rev); err != nil {
			return fmt.Errorf("unable to scan rev version data: %w", err)
		}
		ah.versionRev = rev
	}

	ah.version = MakeArchiveVersion(major, minor, rev)

	if ah.version < BackupVersions["1.0"] || ah.version > BackupVersions["1.15"] {
		return fmt.Errorf("unsupported archive version %d.%d", major, minor)
	}

	intSize, err := ah.ReadByte()
	if err != nil {
		return fmt.Errorf("cannot read intSize value: %w", err)
	}
	ah.intSize = uint32(intSize)

	if ah.version >= BackupVersions["1.7"] {
		offSize, err := ah.ReadByte()
		if err != nil {
			return fmt.Errorf("cannot read intSize value: %w", err)
		}
		ah.offSize = uint32(offSize)
	} else {
		ah.offSize = ah.intSize
	}

	if err := ah.ScanBytes(&format); err != nil {
		return fmt.Errorf("unable to scan bytes from TOC srcFile: %w", err)
	}
	if ArchTar != format {
		return fmt.Errorf("unsupported format \"%s\" suports only directory", BackupFormats[format])
	}

	if ah.version >= BackupVersions["1.15"] {
		algorithm, err := ah.ReadByte()
		if err != nil {
			return fmt.Errorf("unable to scan CompressionSpec.Algorithm: %w", err)
		}
		ah.CompressionSpec.Algorithm = int32(algorithm)
		ah.compression = int32(algorithm)
	} else if ah.version >= BackupVersions["1.2"] {
		if ah.version < BackupVersions["1.4"] {
			level, err := ah.ReadByte()
			if err != nil {
				return fmt.Errorf("unable to scan CompressionSpec.Level: %w", err)
			}
			ah.CompressionSpec.Level = int32(level)
		} else {
			if err = ah.ScanInt(&ah.CompressionSpec.Level); err != nil {
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
		if err = ah.ScanInt(&tmSec, &tmMin, &tmHour, &tmDay, &tmMon, &tmYear, &tmIsDst); err != nil {
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
		archDbName, err := ah.ReadStr()
		if err != nil {
			return fmt.Errorf("cannot read archdbname: %w", err)
		}
		ah.archDbName = archDbName
	}

	if ah.version >= BackupVersions["1.10"] {
		archiveRemoteVersion, err := ah.ReadStr()
		if err != nil {
			return fmt.Errorf("cannot rad archiveRemoteVersion: %w", err)
		}
		ah.archiveRemoteVersion = archiveRemoteVersion

		archiveDumpVersion, err := ah.ReadStr()
		if err != nil {
			return fmt.Errorf("cannot read archiveDumpVersion: %w", err)
		}
		ah.archiveDumpVersion = archiveDumpVersion
	}

	return nil
}

func (ah *ArchiveHandle) ReadStr() (*string, error) {
	l, err := ah.ReadInt()
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

func (ah *ArchiveHandle) GetCurFilePos() int64 {
	pos, _ := ah.destFile.Seek(0, io.SeekCurrent)
	return pos
}

func (ah *ArchiveHandle) ReadInt() (int32, error) {
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
		sign, err = ah.ReadByte()
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

func (ah *ArchiveHandle) ReadByte() (byte, error) {
	res, err := ah.ReadBytes(1)
	return res[0], err
}

func (ah *ArchiveHandle) ReadBytes(length int) ([]byte, error) {
	bytes := make([]byte, length)
	if _, err := ah.srcFile.Read(bytes); err != nil {
		return nil, err
	}
	return bytes, nil
}

func (ah *ArchiveHandle) ScanBytes(byteVars ...*byte) error {
	bytes, err := ah.ReadBytes(len(byteVars))
	if err != nil {
		return err
	}
	for idx, _ := range bytes {
		//var copiedVal = bytes[idx]
		*byteVars[idx] = bytes[idx]
	}

	return nil
}

func (ah *ArchiveHandle) ScanInt(byteVars ...*int32) error {

	for idx, _ := range byteVars {
		val, err := ah.ReadInt()
		if err != nil {
			return err
		}
		*byteVars[idx] = val
	}

	return nil
}

func (ah *ArchiveHandle) ReadToc() error {
	//var tmp string
	//var DumpId int32
	//var depIdx int32
	//var depSize int32
	//var te *Entry
	//var isSupported bool

	if err := ah.ScanInt(&ah.tocCount); err != nil {
		return fmt.Errorf("cannot scan tocCount: %w", err)
	}
	ah.maxDumpId = 0

	tocList := make([]Entry, 0)

	for i := int32(0); i < ah.tocCount; i++ {
		te := Entry{}
		if err := ah.ScanInt(&te.dumpId); err != nil {
			return fmt.Errorf("cannot scan tocCount: %w", err)
		}

		if te.dumpId <= 0 {
			return fmt.Errorf("entry ID %d out of range perhaps a corrupt TOC", te.dumpId)
		}
		if te.dumpId > ah.maxDumpId {
			ah.maxDumpId = te.dumpId
		}

		if err := ah.ScanInt(&te.hadDumper); err != nil {
			return fmt.Errorf("cannot scan hadDumer data: %w", err)
		}

		if ah.version >= BackupVersions["1.8"] {
			tmp, err := ah.ReadStr()
			if err != nil {
				return fmt.Errorf("cannot read catalogId: %w", err)
			}
			if tmp == nil {
				return errors.New("unexpected nil pointer")
			}
			tableOid, err := strconv.ParseUint(*tmp, 10, 32)
			if err != nil {
				return fmt.Errorf("cannot cast str to uint32: %w", err)
			}
			te.catalogId.tableOid = Oid(tableOid)
		} else {
			te.catalogId.tableOid = InvalidOid
		}
		tmp, err := ah.ReadStr()
		if err != nil {
			return fmt.Errorf("cannot read catalogId: %w", err)
		}
		if tmp == nil {
			return errors.New("unexpected nil pointer")
		}
		oid, err := strconv.ParseUint(*tmp, 10, 32)
		if err != nil {
			return fmt.Errorf("cannot cast str to uint32: %w", err)
		}
		te.catalogId.oid = Oid(oid)

		tag, err := ah.ReadStr()
		if err != nil {
			return fmt.Errorf("cannot read tag: %w", err)
		}
		te.tag = tag

		desc, err := ah.ReadStr()
		if err != nil {
			return fmt.Errorf("cannot read desc: %w", err)
		}
		te.desc = desc

		if ah.version >= BackupVersions["1.11"] {
			if err = ah.ScanInt(&te.section); err != nil {
				return fmt.Errorf("cannot section: %w", err)
			}
		} else {
			return errors.New("unsupported version")
		}

		defn, err := ah.ReadStr()
		if err != nil {
			return fmt.Errorf("cannot read defn: %w", err)
		}
		te.defn = defn

		dropStmt, err := ah.ReadStr()
		if err != nil {
			return fmt.Errorf("cannot read dropStmt: %w", err)
		}
		te.dropStmt = dropStmt

		if ah.version >= BackupVersions["1.3"] {
			copyStmt, err := ah.ReadStr()
			if err != nil {
				return fmt.Errorf("cannot read defn: %w", err)
			}
			te.copyStmt = copyStmt
		}

		if ah.version >= BackupVersions["1.6"] {
			namespace, err := ah.ReadStr()
			if err != nil {
				return fmt.Errorf("cannot read namespace: %w", err)
			}
			te.namespace = namespace
		}

		if ah.version >= BackupVersions["1.10"] {
			tablespace, err := ah.ReadStr()
			if err != nil {
				return fmt.Errorf("cannot read tablespace: %w", err)
			}
			te.tablespace = tablespace
		}

		if ah.version >= BackupVersions["1.14"] {
			tableam, err := ah.ReadStr()
			if err != nil {
				return fmt.Errorf("cannot read tableam: %w", err)
			}
			te.tableam = tableam
		}

		owner, err := ah.ReadStr()
		if err != nil {
			return fmt.Errorf("cannot read tablespace: %w", err)
		}
		te.owner = owner

		isSupported := true
		if ah.version < BackupVersions["1.9"] {
			isSupported = false
		} else {
			tmp, err := ah.ReadStr()
			if err != nil {
				return fmt.Errorf("cannot read catalogId: %w", err)
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
			return errors.New("unsupported version")
		}

		/* Read TOC entry dependencies */
		if ah.version >= BackupVersions["1.5"] {
			te.dependencies = make([]int32, 0, 10)
			for {
				tmp, err = ah.ReadStr()
				if err != nil {
					return fmt.Errorf("cannot read catalogId: %w", err)
				}
				if tmp == nil {
					break
				}

				val, err := strconv.ParseInt(*tmp, 10, 32)
				if err != nil {
					return fmt.Errorf("unable to parse dependency int32 value: %w", err)
				}

				te.dependencies = append(te.dependencies, int32(val))
			}
			te.nDeps = int32(len(te.dependencies))

		} else {
			te.dependencies = nil
			te.nDeps = 0
		}

		te.dataLength = 0

		fileName, err := ah.ReadStr()
		if err != nil {
			return fmt.Errorf("cannot additional data fileName: %w", err)
		}
		te.fileName = fileName

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

	log.Printf("tocLen = %d\n", ah.toc.Len())
	testToc(ah.toc, tocList)

	return nil
}

func testToc(r *ring.Ring, tocLIst []Entry) {
	idx := 0
	r.Do(func(a any) {
		//fmt.Printf("%s\n", tocLIst[idx])
		fmt.Printf("%s\n", a)
		idx++
	})
}

func (ah *ArchiveHandle) GetEntries() *ring.Ring {
	return ah.toc
}

func (ah *ArchiveHandle) SetEntries(entries *ring.Ring) {
	ah.toc = entries
}

// Write methods

func (ah *ArchiveHandle) WriteHead() error {

	if err := ah.WriteBuf([]byte("PGDMP")); err != nil {
		return fmt.Errorf("cannot write magic str: %w", err)
	}

	if err := ah.WriteByte(ah.versionMajor); err != nil {
		return fmt.Errorf("cannot write versionMajor: %w", err)
	}

	if err := ah.WriteByte(ah.versionMinor); err != nil {
		return fmt.Errorf("cannot write versionMinor: %w", err)
	}

	if err := ah.WriteByte(ah.versionRev); err != nil {
		return fmt.Errorf("cannot write versionRev: %w", err)
	}

	if err := ah.WriteByte(byte(ah.intSize)); err != nil {
		return fmt.Errorf("cannot write intSize: %w", err)
	}

	if err := ah.WriteByte(byte(ah.offSize)); err != nil {
		return fmt.Errorf("cannot write offSize: %w", err)
	}

	if err := ah.WriteByte(ArchTar); err != nil {
		return fmt.Errorf("cannot write format: %w", err)
	}

	var compressionNotSet int32 = -1
	if err := ah.WriteInt(compressionNotSet); err != nil {
		return fmt.Errorf("cannot write CompressionSpec.Algorithm: %w", err)
	}

	if err := ah.WriteInt(ah.crtmDateTime.TmSec); err != nil {
		return fmt.Errorf("cannot write TmSec: %w", err)
	}

	if err := ah.WriteInt(ah.crtmDateTime.TmMin); err != nil {
		return fmt.Errorf("cannot write TmMin: %w", err)
	}
	if err := ah.WriteInt(ah.crtmDateTime.TmHour); err != nil {
		return fmt.Errorf("cannot write TmHour: %w", err)
	}
	if err := ah.WriteInt(ah.crtmDateTime.TmMday); err != nil {
		return fmt.Errorf("cannot write TmMday: %w", err)
	}
	if err := ah.WriteInt(ah.crtmDateTime.TmMon); err != nil {
		return fmt.Errorf("cannot write TmMon: %w", err)
	}
	if err := ah.WriteInt(ah.crtmDateTime.TmYear); err != nil {
		return fmt.Errorf("cannot write TmYear: %w", err)
	}
	if err := ah.WriteInt(ah.crtmDateTime.TmIsDst); err != nil {
		return fmt.Errorf("cannot write TmIsDst: %w", err)
	}
	//connectionString := ""
	if err := ah.WriteStr(ah.archDbName); err != nil {
		return fmt.Errorf("cannot write archDbName: %w", err)
	}
	if err := ah.WriteStr(ah.archiveRemoteVersion); err != nil {
		return fmt.Errorf("cannot write archiveRemoteVersion: %w", err)
	}
	if err := ah.WriteStr(ah.archiveDumpVersion); err != nil {
		return fmt.Errorf("cannot write archiveDumpVersion: %w", err)
	}

	return nil
}

// 4
func (ah *ArchiveHandle) WriteToc() error {
	log.Printf("pos = %d", ah.GetCurFilePos())
	var tocCount int32
	ah.toc.Do(func(a any) {
		tocCount++
	})
	if err := ah.WriteInt(tocCount); err != nil {
		return fmt.Errorf("cannot write tocCount: %w", err)
	}

	for _, te := range ah.tocList {

		if err := ah.WriteInt(te.dumpId); err != nil {
			panic(fmt.Sprintf("unable to write dumpId: %s", err))
		}

		if err := ah.WriteInt(te.hadDumper); err != nil {
			panic(fmt.Sprintf("unable to write dataDumper: %s", err))
		}

		//oidStr := fmt.Sprintf("%d", te.catalogId.tableOid)
		oidStr := strconv.FormatUint(uint64(te.catalogId.tableOid), 10)
		if err := ah.WriteStr(&oidStr); err != nil {
			panic(fmt.Sprintf("unable to write tableOid: %s", err))
		}

		oidStr = strconv.FormatUint(uint64(te.catalogId.oid), 10)
		if err := ah.WriteStr(&oidStr); err != nil {
			panic(fmt.Sprintf("unable to write Oid: %s", err))
		}

		if err := ah.WriteStr(te.tag); err != nil {
			panic(fmt.Sprintf("unable to write tag: %s", err))
		}
		if err := ah.WriteStr(te.desc); err != nil {
			panic(fmt.Sprintf("unable to write desc: %s", err))
		}
		if err := ah.WriteInt(te.section); err != nil {
			panic(fmt.Sprintf("unable to write section: %s", err))
		}
		if err := ah.WriteStr(te.defn); err != nil {
			panic(fmt.Sprintf("unable to write defn: %s", err))
		}
		if err := ah.WriteStr(te.dropStmt); err != nil {
			panic(fmt.Sprintf("unable to write dropStmt: %s", err))
		}
		if err := ah.WriteStr(te.copyStmt); err != nil {
			panic(fmt.Sprintf("unable to write copyStmt: %s", err))
		}
		if err := ah.WriteStr(te.namespace); err != nil {
			panic(fmt.Sprintf("unable to write namespace: %s", err))
		}
		if err := ah.WriteStr(te.tablespace); err != nil {
			panic(fmt.Sprintf("unable to write tablespace: %s", err))
		}
		if err := ah.WriteStr(te.tableam); err != nil {
			panic(fmt.Sprintf("unable to write tableam: %s", err))
		}
		if err := ah.WriteStr(te.owner); err != nil {
			panic(fmt.Sprintf("unable ro write owner: %s", err))
		}
		someFalseValue := "false"
		if err := ah.WriteStr(&someFalseValue); err != nil {
			panic(fmt.Sprintf("unable to write \"false\" value: %s", err))
		}

		for _, d := range te.dependencies {
			depStr := strconv.FormatInt(int64(d), 10)
			if err := ah.WriteStr(&depStr); err != nil {
				panic(fmt.Sprintf("unable to write entry dependency value: %s", err))
			}
		}
		/* Terminate List */
		if err := ah.WriteStr(nil); err != nil {
			panic(fmt.Sprintf("unable to write entry dependencies list terminator: %s", err))
		}

		// WriteExtraTocPtr - write filename here
		if err := ah.WriteStr(te.fileName); err != nil {
			panic(fmt.Sprintf("unable to write fileName: %s", err))
		}

	}

	return nil
}

func (ah *ArchiveHandle) WriteBuf(buf []byte) error {
	n, err := ah.destFile.Write(buf)
	ah.tocWrittenBytes += n
	if err != nil {
		return err
	}

	return nil
}

func (ah *ArchiveHandle) WriteByte(data byte) error {
	err := ah.WriteBuf([]byte{data})
	if err != nil {
		return fmt.Errorf("cannot write byte data: %w", err)
	}
	return nil
}

func (ah *ArchiveHandle) WriteInt(i int32) error {
	var b int32
	var sign byte

	if i < 0 {
		sign = 1
		i = -i
	}

	if err := ah.WriteByte(sign); err != nil {
		return fmt.Errorf("unable to write sign byte: %w", err)
	}

	for b = 0; b < int32(ah.intSize); b++ {
		if err := ah.WriteByte(byte(i) & 0xFF); err != nil {
			return fmt.Errorf("unable to write int byte: %w", err)
		}
		i >>= 8
	}
	return nil
}

func (ah *ArchiveHandle) WriteStr(data *string) error {

	if data != nil {
		if err := ah.WriteInt(int32(len([]byte(*data)))); err != nil {
			return fmt.Errorf("unable to write str length: %w", err)
		}
		if err := ah.WriteBuf([]byte(*data)); err != nil {
			return fmt.Errorf("unable to write string buffer: %w", err)
		}
	} else {

		if err := ah.WriteInt(-1); err != nil {
			return fmt.Errorf("unable to write empty string: %w", err)
		}
	}
	return nil
}

func (ah *ArchiveHandle) ReadFile() error {
	if err := ah.ReadHead(); err != nil {
		return err
	}

	if err := ah.ReadToc(); err != nil {
		return err
	}
	return nil
}

func (ah *ArchiveHandle) WriteFile() error {
	if err := ah.WriteHead(); err != nil {
		return err
	}

	if err := ah.WriteToc(); err != nil {
		return err
	}
	return nil
}
