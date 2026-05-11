package entries

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
)

func TestSequence_Entry(t *testing.T) {
	s := &Sequence{
		Schema:    "public",
		Name:      "my_sequence",
		Owner:     "my_owner",
		IsCalled:  true,
		LastValue: 1,
		Oid:       2,
	}
	entry, err := s.Entry()
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "\"my_sequence\"", *entry.Tag)
	assert.Equal(t, "\"public\"", *entry.Namespace)
	assert.Equal(t, "\"my_owner\"", *entry.Owner)
	assert.Equal(t, "SELECT pg_catalog.setval('\"public\".\"my_sequence\"', 1, true);", *entry.Defn)
	assert.Equal(t, int32(0), entry.HadDumper)
	assert.Equal(t, toc.Oid(0), entry.CatalogId.Oid)
	assert.Equal(t, int32(0), entry.NDeps)
	assert.Equal(t, 0, len(entry.Dependencies))
	assert.NotNil(t, entry.FileName)
	assert.NotNil(t, entry.DropStmt)
}
