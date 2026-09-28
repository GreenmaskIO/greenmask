// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/internal/utils/testutils"
)

// noticeSuite checks the whole path a server message travels: PostgreSQL raises
// it, pgx turns it into a NoticeResponse, the handler on the connection logs it.
// The unit tests only prove the handler is wired to the config; nothing there
// would notice if pgx stopped calling it.
type noticeSuite struct {
	testutils.PgContainerSuite
}

func (s *noticeSuite) TestOpenConnLogsServerNotices() {
	ctx := context.Background()
	buf := captureLog(s.T(), zerolog.DebugLevel)

	conn, err := openConn(ctx, s.GetConnectionString(ctx))
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(conn.Close(ctx))
	}()

	// The shape a restore script uses to report what it did.
	_, err = conn.Exec(ctx, `
		DO $$
		BEGIN
		  RAISE NOTICE 'Re-applied grants: schema=%', 'fault';
		  RAISE WARNING 'nothing matched';
		END $$;`)
	s.Require().NoError(err)

	logged := buf.String()
	s.Assert().Contains(logged, "Re-applied grants: schema=fault")
	s.Assert().Contains(logged, `"level":"info"`)
	s.Assert().Contains(logged, "nothing matched")
	s.Assert().Contains(logged, `"level":"warn"`)
}

// A plain pgx.Connect drops the same message. This is the behaviour the change
// exists to correct, pinned so the test above cannot pass for the wrong reason.
func (s *noticeSuite) TestPlainConnectionDropsNotices() {
	ctx := context.Background()
	buf := captureLog(s.T(), zerolog.DebugLevel)

	conn, err := s.GetConnection(ctx) // uses pgx.Connect, no notice handler
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(conn.Close(ctx))
	}()

	_, err = conn.Exec(ctx, `DO $$ BEGIN RAISE NOTICE 'dropped on the floor'; END $$;`)
	s.Require().NoError(err)

	s.Assert().NotContains(buf.String(), "dropped on the floor")
}

func (s *noticeSuite) TestNoticesRespectLogLevel() {
	ctx := context.Background()
	buf := captureLog(s.T(), zerolog.WarnLevel)

	conn, err := openConn(ctx, s.GetConnectionString(ctx))
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(conn.Close(ctx))
	}()

	_, err = conn.Exec(ctx, `
		DO $$
		BEGIN
		  RAISE NOTICE 'hidden at warn';
		  RAISE WARNING 'shown at warn';
		END $$;`)
	s.Require().NoError(err)

	logged := buf.String()
	s.Assert().NotContains(logged, "hidden at warn")
	s.Assert().Contains(logged, "shown at warn")
}

func TestNotices(t *testing.T) {
	suite.Run(t, new(noticeSuite))
}
