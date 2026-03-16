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

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	mysqlrestoreconfig "github.com/greenmaskio/greenmask/pkg/mysql/restore/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/restorers"
)

var (
	errUnknownDumpFormat = errors.New("unknown dump format")
)

type dummyTaskMapper struct{}

func (*dummyTaskMapper) SetTaskCompleted(_ models.TaskID) {
	// no-op
}

func (*dummyTaskMapper) IsTaskCompleted(_ models.TaskID) bool {
	return true
}

type Producer struct {
	meta    models.Metadata
	st      interfaces.Storager
	opt     mysqlrestoreconfig.RestoreOptions
	err     error
	lastIdx int
	taskIDs []models.TaskID
}

func New(
	meta models.Metadata,
	st interfaces.Storager,
	opt mysqlrestoreconfig.RestoreOptions,
) *Producer {
	taskIDs := make([]models.TaskID, 0, len(meta.DumpStat.RestorationItems))
	for taskID := range meta.DumpStat.RestorationItems {
		taskIDs = append(taskIDs, taskID)
	}
	return &Producer{
		meta:    meta,
		st:      st,
		opt:     opt,
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

func (p *Producer) Task() (interfaces.Restorer, error) {
	if p.err != nil {
		return nil, p.err
	}
	taskID := p.taskIDs[p.lastIdx]
	restorationItem, ok := p.meta.DumpStat.RestorationItems[taskID]
	if !ok {
		panic("no restoration item")
	}
	switch restorationItem.ObjectKind {
	case models.ObjectKindTable:
		opts := []restorers.Option{
			restorers.WithCompression(
				restorationItem.Compression == models.CompressionGzip ||
					restorationItem.Compression == models.CompressionPgzip,
				restorationItem.Compression == models.CompressionPgzip,
			),
			restorers.WithWarnings(p.opt.PrintWarnings, p.opt.MaxFetchWarnings),
		}

		stat := p.meta.DumpStat.TaskStats[taskID]
		switch stat.ObjectStat.Format {
		case models.DumpFormatInsert:
			return restorers.NewTableDataRestorerInsert(
				restorationItem, p.opt.ConnectionOpts, p.st, &dummyTaskMapper{}, opts...,
			)
		case models.DumpFormatCsv:
			return restorers.NewTableDataRestorerCsv(
				restorationItem, p.opt.ConnectionOpts, p.st, &dummyTaskMapper{}, opts...,
			)
		default:
			return nil, fmt.Errorf("dump fomat ='%s': %w", stat.ObjectStat.Format, errUnknownDumpFormat)
		}
	}
	return nil, fmt.Errorf("create restore task for kind '%s': %w",
		restorationItem.ObjectKind, errUnsupportedObjectKind)
}
