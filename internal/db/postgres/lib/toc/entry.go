package toc

const (
	SectionNone     int32 = iota + 1
	SectionPreData        /* stuff to be processed before data */
	SectionData           /* table data, large objects, LO comments */
	SectionPostData       /* stuff to be processed after data */
)

type Oid int

type CatalogId struct {
	TableOid Oid
	Oid      Oid
}

type Entry struct {
	CatalogId CatalogId
	DumpId    int32
	Section   int32
	HadDumper int32 /* Archiver was passed a dumper routine (used
	 * in restore) */
	Tag        *string /* index Tag */
	Namespace  *string /* null or empty string if not in a schema */
	Tablespace *string /* null if not in a Tablespace; empty string
	 * means use database default */
	Tableam      *string /* table access method, only for TABLE tags */
	Owner        *string
	Desc         *string
	Defn         *string
	DropStmt     *string
	CopyStmt     *string
	Dependencies []int32 /* dumpIds of objects this one depends on */
	NDeps        int32   /* number of Dependencies */
	FileName     *string

	DataDumper int32 /* Routine to dump data for object */

	/* working state while dumping/restoring */
	// TODO: Pay attention, maybe you need to change this byte latter
	DataLength uint32 /* item's data size; 0 if none or unknown */

}

type Entries []*Entry
