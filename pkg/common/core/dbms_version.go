package core

// Well-known DBMSVersion.Metadata keys.
const (
	// DBMSVendorKey is the Metadata key under which the server vendor is stored
	// (e.g. for MySQL-compatible servers: "mysql" or "mariadb").
	DBMSVendorKey = "vendor"
	// DBMSVersionCommentKey is the Metadata key for the raw server version
	// comment (e.g. "MySQL Community Server - GPL").
	DBMSVersionCommentKey = "version_comment"
)

// Known server vendors stored under DBMSVendorKey.
const (
	DBMSVendorMySQL   = "mysql"
	DBMSVendorMariaDB = "mariadb"
)

type DBMSVersion struct {
	FullString string
	Major      int
	Minor      int
	Patch      int
	Metadata   map[string]string
}

// Vendor returns the server vendor recorded in Metadata, or "" if unknown.
func (v DBMSVersion) Vendor() string {
	return v.Metadata[DBMSVendorKey]
}
