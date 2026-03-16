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
	"time"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/restore/taskmapper"

	mysqlrestoreconfig "github.com/greenmaskio/greenmask/pkg/mysql/restore/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/restorers"
)

const (
	defaultTaskCompletionRefreshTime = 500 * time.Millisecond
)

var (
	errUnsupportedObjectKind = errors.New("unsupported object kind")
)

type taskMapper interface {
	IsTaskCompleted(taskID models.TaskID) bool
	SetTaskCompleted(taskID models.TaskID)
}

type ProducerWithOrder struct {
	meta         models.Metadata
	st           interfaces.Storager
	opt          mysqlrestoreconfig.RestoreOptions
	err          error
	lastIdx      int
	taskResolver interfaces.TaskMapper
}

func NewWithOrder(
	meta models.Metadata,
	st interfaces.Storager,
	opt mysqlrestoreconfig.RestoreOptions,
	taskResolver *taskmapper.TaskResolver,
) *ProducerWithOrder {
	return &ProducerWithOrder{
		meta:         meta,
		st:           st,
		opt:          opt,
		taskResolver: taskResolver,
		lastIdx:      -1,
	}
}

func (p *ProducerWithOrder) Err() error {
	return p.err
}

func allDependenciesAreCompleted(taskMapper taskMapper, dependencies []models.TaskID) bool {
	for _, dependency := range dependencies {
		if !taskMapper.IsTaskCompleted(dependency) {
			return false
		}
	}
	return true
}

func (p *ProducerWithOrder) waitForTasks(ctx context.Context, dependencies []models.TaskID) {
	t := time.NewTicker(defaultTaskCompletionRefreshTime)
	for {
		select {
		case <-ctx.Done():
			p.err = ctx.Err()
		case <-t.C:
			if allDependenciesAreCompleted(p.taskResolver, dependencies) {
				return
			}
		}
	}
}

// Next - moves to the next task.
//
// TODO: if we have an issue with the graph or some tables are missing in the dump
// we can get stuck here forever. Consider add additional checks.
func (p *ProducerWithOrder) Next(ctx context.Context) bool {
	if p.err != nil {
		return false
	}
	p.lastIdx++
	if len(p.meta.DumpStat.RestorationContext.RestorationOrder) == 0 ||
		len(p.meta.DumpStat.RestorationContext.RestorationOrder) == p.lastIdx {
		// If there are no tasks or we have reached the end of the tasks.
		// Return false to stop the iteration.
		return false
	}
	currentTaskID := p.meta.DumpStat.RestorationContext.RestorationOrder[p.lastIdx]
	dependencies := p.meta.DumpStat.RestorationContext.TaskDependencies[currentTaskID]
	p.waitForTasks(ctx, dependencies)
	return true
}

func (p *ProducerWithOrder) Task() (interfaces.Restorer, error) {
	if p.err != nil {
		return nil, p.err
	}
	currentTaskID := p.meta.DumpStat.RestorationContext.RestorationOrder[p.lastIdx]

	restorationItem, ok := p.meta.DumpStat.RestorationItems[currentTaskID]
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
		stat := p.meta.DumpStat.TaskStats[currentTaskID]
		switch stat.ObjectStat.Format {
		case models.DumpFormatInsert:
			return restorers.NewTableDataRestorerInsert(
				restorationItem, p.opt.ConnectionOpts, p.st, p.taskResolver, opts...,
			)
		case models.DumpFormatCsv:
			return restorers.NewTableDataRestorerCsv(
				restorationItem, p.opt.ConnectionOpts, p.st, p.taskResolver, opts...,
			)
		default:
			return nil, fmt.Errorf("dump format ='%s': %w", stat.ObjectStat.Format, errUnknownDumpFormat)
		}
	}
	return nil, fmt.Errorf("create restore task for kind '%s': %w",
		restorationItem.ObjectKind, errUnsupportedObjectKind)
}
