package toc

const (
	SectionNone     int32 = iota + 1
	SectionPreData        /* stuff to be processed before data */
	SectionData           /* table data, large objects, LO comments */
	SectionPostData       /* stuff to be processed after data */
)

type Oid int

type CatalogId struct {
	tableOid Oid
	oid      Oid
}

type Entry struct {
	prev      *Entry
	next      *Entry
	catalogId CatalogId
	dumpId    int32
	section   int32
	hadDumper int32 /* Archiver was passed a dumper routine (used
	 * in restore) */
	tag        *string /* index tag */
	namespace  *string /* null or empty string if not in a schema */
	tablespace *string /* null if not in a tablespace; empty string
	 * means use database default */
	tableam      *string /* table access method, only for TABLE tags */
	owner        *string
	desc         *string
	defn         *string
	dropStmt     *string
	copyStmt     *string
	dependencies []int32 /* dumpIds of objects this one depends on */
	nDeps        int32   /* number of dependencies */
	fileName     *string

	dataDumper int32 /* Routine to dump data for object */
	//const void *dataDumperArg /* Arg for above routine */
	//void       *formatData    /* TOC Entry data specific to file format */

	/* working state while dumping/restoring */
	dataLength uint32 /* item's data size; 0 if none or unknown */
	//int     reqs       /* do we need schema and/or data of object
	// * (REQ_* bit mask) */
	// TODO: Pay attention, maybe you need to change this byte latter
	//bool created /* set for DATA member if TABLE was created */
}
