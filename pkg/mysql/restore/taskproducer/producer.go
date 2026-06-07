// Copyright 2025 Greenmask
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

package taskproducer

import (
	"context"
	"errors"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/restorers"
)

var (
	errUnknownDumpFormat = errors.New("unknown dump format")
)

type RestoreOptions struct {
	PrintWarnings           bool
	MaxFetchWarnings        int
	DisableForeignKeyChecks bool
	DisableUniqueChecks     bool
	InsertIgnore            bool
	InsertReplace           bool
	MaxInsertStatementSize  int
	DatabaseRemap           map[string]string
}

type dummyTaskMapper struct{}

func (*dummyTaskMapper) SetTaskCompleted(_ core.TaskID) {
	// no-op
}

func (*dummyTaskMapper) IsTaskCompleted(_ core.TaskID) bool {
	return true
}

type Producer struct {
	meta    core.Metadata
	st      core.Storager
	conn    *mysqlmodels.ConnConfig
	opts    RestoreOptions
	err     error
	lastIdx int
	taskIDs []core.TaskID
}

func New(
	meta core.Metadata,
	st core.Storager,
	conn *mysqlmodels.ConnConfig,
	opts RestoreOptions,
) *Producer {
	var taskIDs []core.TaskID
	if meta.DataDump != nil {
		taskIDs = make([]core.TaskID, 0, len(meta.DataDump.DumpStat.RestorationItems))
		for taskID := range meta.DataDump.DumpStat.RestorationItems {
			taskIDs = append(taskIDs, taskID)
		}
	}

	return &Producer{
		meta:    meta,
		st:      st,
		conn:    conn,
		opts:    opts,
		taskIDs: taskIDs,
		lastIdx: -1,
	}
}

func (p *Producer) Err() error {
	return p.err
}

func (p *Producer) Next(_ context.Context) bool {
	if p.err != nil {
		return false
	}
	p.lastIdx++
	if len(p.taskIDs) == 0 ||
		p.lastIdx == len(p.taskIDs) {
		// If there are no tasks or we have reached the end of the tasks.
		// Return false to stop the iteration.
		return false
	}
	return true
}

func (p *Producer) Task() (core.Restorer, error) {
	if p.err != nil {
		return nil, p.err
	}
	taskID := p.taskIDs[p.lastIdx]
	restorationItem, ok := p.meta.DataDump.DumpStat.RestorationItems[taskID]
	if !ok {
		panic("no restoration item")
	}
	switch restorationItem.ObjectKind {
	case core.ObjectKindTable:
		opts := []restorers.Option{
			restorers.WithCompression(
				restorationItem.Compression.IsEnabled(),
				restorationItem.Compression.IsPgzip(),
			),
			restorers.WithWarnings(p.opts.PrintWarnings, p.opts.MaxFetchWarnings),
			restorers.WithForeignKeyChecks(p.opts.DisableForeignKeyChecks),
			restorers.WithUniqueChecks(p.opts.DisableUniqueChecks),
			restorers.WithMaxInsertStatementSize(p.opts.MaxInsertStatementSize),
		}
		if p.opts.InsertIgnore {
			opts = append(opts, restorers.WithInsertIgnore())
		}
		if p.opts.InsertReplace {
			opts = append(opts, restorers.WithInsertReplace())
		}
		if len(p.opts.DatabaseRemap) > 0 {
			opts = append(opts, restorers.WithDatabaseRemap(p.opts.DatabaseRemap))
		}

		stat := p.meta.DataDump.DumpStat.TaskStats[taskID]
		switch stat.ObjectStat.Format {
		case core.DumpFormatInsert:
			return restorers.NewTableDataRestorerInsert(
				restorationItem, p.conn, p.st, &dummyTaskMapper{}, opts...,
			)
		case core.DumpFormatCsv:
			return restorers.NewTableDataRestorerCsv(
				restorationItem, p.conn, p.st, &dummyTaskMapper{}, opts...,
			)
		default:
			return nil, fmt.Errorf("dump format ='%s': %w", stat.ObjectStat.Format, errUnknownDumpFormat)
		}
	}
	return nil, fmt.Errorf("create restore task for kind '%s': %w",
		restorationItem.ObjectKind, errUnsupportedObjectKind)
}
