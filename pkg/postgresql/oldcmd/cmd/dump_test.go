package cmd

import (
	"testing"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDump_MergeTocEntries(t *testing.T) {
	// helper to build minimal TOC entries
	e := func(id int32, section int32) *toc.Entry {
		return &toc.Entry{
			DumpId:  id,
			Section: section,
		}
	}

	dumpIds := func(entries []*toc.Entry) []int32 {
		out := make([]int32, 0, len(entries))
		for _, e := range entries {
			out = append(out, e.DumpId)
		}
		return out
	}

	t.Run("no data entries returns schema as-is", func(t *testing.T) {
		d := &Dump{}

		schema := []*toc.Entry{
			e(1, toc.SectionPreData),
			e(2, toc.SectionPreData),
			e(3, toc.SectionPostData),
		}

		res, err := d.MergeTocEntries(schema, nil)
		require.NoError(t, err)
		assert.Equal(t, dumpIds(schema), dumpIds(res))
	})

	t.Run("all sections exist predata data postdata", func(t *testing.T) {
		d := &Dump{}

		schema := []*toc.Entry{
			e(1, toc.SectionPreData),
			e(2, toc.SectionPreData),
			e(3, toc.SectionPostData),
			e(4, toc.SectionPostData),
		}

		data := []*toc.Entry{
			e(10, toc.SectionData),
			e(11, toc.SectionData),
		}

		res, err := d.MergeTocEntries(schema, data)
		require.NoError(t, err)

		assert.Equal(
			t,
			[]int32{1, 2, 10, 11, 3, 4},
			dumpIds(res),
		)
	})

	t.Run("no postdata section", func(t *testing.T) {
		d := &Dump{}

		schema := []*toc.Entry{
			e(1, toc.SectionPreData),
			e(2, toc.SectionPreData),
			e(3, toc.SectionPreData),
		}

		data := []*toc.Entry{
			e(10, toc.SectionData),
		}

		res, err := d.MergeTocEntries(schema, data)
		require.NoError(t, err)

		// PreData -> Data, no duplication
		assert.Equal(
			t,
			[]int32{1, 2, 3, 10},
			dumpIds(res),
		)
	})

	t.Run("no predata section", func(t *testing.T) {
		d := &Dump{}

		schema := []*toc.Entry{
			e(3, toc.SectionPostData),
			e(4, toc.SectionPostData),
		}

		data := []*toc.Entry{
			e(10, toc.SectionData),
		}

		res, err := d.MergeTocEntries(schema, data)
		require.NoError(t, err)

		// Data -> PostData
		assert.Equal(
			t,
			[]int32{10, 3, 4},
			dumpIds(res),
		)
	})

	t.Run("schema without sections only data returned", func(t *testing.T) {
		d := &Dump{}

		schema := []*toc.Entry{
			e(1, toc.SectionNone),
			e(2, toc.SectionNone),
		}

		data := []*toc.Entry{
			e(10, toc.SectionData),
		}

		res, err := d.MergeTocEntries(schema, data)
		require.NoError(t, err)

		assert.Equal(
			t,
			[]int32{10},
			dumpIds(res),
		)
	})

	t.Run("regression no duplicated schema entries", func(t *testing.T) {
		d := &Dump{}

		schema := []*toc.Entry{
			e(1, toc.SectionPreData),
			e(2, toc.SectionPreData),
		}

		data := []*toc.Entry{
			e(10, toc.SectionData),
		}

		res, err := d.MergeTocEntries(schema, data)
		require.NoError(t, err)

		ids := dumpIds(res)

		assert.Equal(t, []int32{1, 2, 10}, ids)
		assert.Len(t, ids, 3)
	})
}
