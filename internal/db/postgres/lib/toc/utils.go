package toc

const (
	ArchUnknown   byte = 0
	ArchCustom    byte = 1
	ArchTar       byte = 3
	ArchNull      byte = 4
	ArchDirectory byte = 5
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
