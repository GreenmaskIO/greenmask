package copy

const defaultCopyDelimiter = '\t'

var defaultNullSeq = []byte("\\N")
var defaultCopyTerminationSeq = []byte("\\.")

type Pos struct {
	start int
	end   int
}

type CopyRow struct {
	positions []Pos
	data      []byte
}
