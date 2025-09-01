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

package restorers

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/db/postgres/utils"
)

type AclRestorer struct {
	Entry *toc.Entry
}

func NewAclRestorer(entry *toc.Entry) *AclRestorer {
	return &AclRestorer{
		Entry: entry,
	}
}

func (ar *AclRestorer) GetEntry() *toc.Entry {
	return ar.Entry
}

func (ar *AclRestorer) Execute(ctx context.Context, conn utils.PGConnector) error {
	err := conn.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if ar.Entry.Defn == nil {
			return fmt.Errorf("received nil pointer instead of ACL definition")
		}
		_, err := tx.Exec(ctx, *ar.Entry.Defn)
		if err != nil {
			return fmt.Errorf("unable to apply ACL: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("cannot commit transaction (restoring %s): %w", ar.DebugInfo(), err)
	}
	return nil
}

func (ar *AclRestorer) DebugInfo() string {
	if ar.Entry.Tag != nil {
		return fmt.Sprintf("ACL %s", *ar.Entry.Tag)
	}
	return "ACL"
}
