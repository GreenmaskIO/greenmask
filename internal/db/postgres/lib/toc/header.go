package toc

type Crtm struct {
	TmSec   int32
	TmMin   int32
	TmHour  int32
	TmMday  int32
	TmMon   int32
	TmYear  int32
	TmIsDst int32
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
