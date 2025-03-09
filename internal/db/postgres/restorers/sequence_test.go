package restorers

import (
	"context"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/db/postgres/utils"
)

func (s *restoresSuite) Test_SequencesRestorer_Execute() {
	ctx := context.Background()
	pgConn, err := s.GetConnection(ctx)
	s.Require().NoError(err)
	conn := utils.NewPGConn(pgConn)

	s.Run("basic", func() {

		_, err = pgConn.Exec(ctx, "CREATE SEQUENCE _test_seq_1231")
		s.Require().NoError(err)

		schemaName := "public"
		seqName := "_test_seq_123"
		data := "SELECT pg_catalog.setval('public._test_seq_1231', 2, false);\n"
		entry := &toc.Entry{
			Namespace: &schemaName,
			Tag:       &seqName,
			Defn:      &data,
		}

		seq := NewSequenceRestorer(entry)
		err = seq.Execute(ctx, conn)
		s.Require().NoError(err)

		var lastVal int
		err = pgConn.QueryRow(ctx, "SELECT last_value FROM _test_seq_1231").
			Scan(&lastVal)
		s.Require().NoError(err)
		s.Require().Equal(2, lastVal)
	})

	s.Run("Defn is nil error expected", func() {

		_, err = pgConn.Exec(ctx, "CREATE SEQUENCE _test_seq_1232")
		s.Require().NoError(err)

		schemaName := "public"
		seqName := "_test_seq_123"
		entry := &toc.Entry{
			Namespace: &schemaName,
			Tag:       &seqName,
		}

		seq := NewSequenceRestorer(entry)
		err = seq.Execute(ctx, conn)
		s.Require().Error(err)

		var lastVal int
		err = pgConn.QueryRow(ctx, "SELECT last_value FROM _test_seq_1232").
			Scan(&lastVal)
		s.Require().NoError(err)
		s.Require().Equal(1, lastVal)
	})
}
