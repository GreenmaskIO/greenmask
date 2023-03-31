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
)

type SequenceRestorer struct {
	Entry *toc.Entry
}

func NewSequenceRestorer(entry *toc.Entry) *SequenceRestorer {
	return &SequenceRestorer{
		Entry: entry,
	}
}

func (td *SequenceRestorer) Execute(ctx context.Context, tx pgx.Tx) error {
	if td.Entry.Defn == nil {
		return fmt.Errorf("received nil pointer intead of sequence")
	}
	_, err := tx.Exec(ctx, *td.Entry.Defn)
	if err != nil {
		return fmt.Errorf("unable to apply sequence set val: %w", err)
	}
	return nil
}

func (td *SequenceRestorer) DebugInfo() string {
	return fmt.Sprintf("sequence %s.%s", *td.Entry.Namespace, *td.Entry.Tag)
}
