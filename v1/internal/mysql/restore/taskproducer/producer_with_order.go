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

	commoninterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/restore/taskmapper"
	mysqlconfig "github.com/greenmaskio/greenmask/v1/internal/mysql/config"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/restore/restorers"
)

const (
	defaultTaskCompletionRefreshTime = 500 * time.Millisecond
)

var (
	errUnsupportedObjectKind = errors.New("unsupported object kind")
)

type taskMapper interface {
	IsTaskCompleted(taskID commonmodels.TaskID) bool
	SetTaskCompleted(taskID commonmodels.TaskID)
}

type ProducerWithOrder struct {
	meta         commonmodels.Metadata
	st           commoninterfaces.Storager
	connCfg      mysqlconfig.ConnectionOpts
	err          error
	lastIdx      int
	taskResolver commoninterfaces.TaskMapper
}

func NewWithOrder(
	meta commonmodels.Metadata,
	st commoninterfaces.Storager,
	connCfg mysqlconfig.ConnectionOpts,
	taskResolver *taskmapper.TaskResolver,
) *ProducerWithOrder {
	return &ProducerWithOrder{
		meta:         meta,
		st:           st,
		connCfg:      connCfg,
		taskResolver: taskResolver,
		lastIdx:      -1,
	}
}

func (p *ProducerWithOrder) Err() error {
	return p.err
}

func allDependenciesAreCompleted(taskMapper taskMapper, dependencies []commonmodels.TaskID) bool {
	for _, dependency := range dependencies {
		if !taskMapper.IsTaskCompleted(dependency) {
			return false
		}
	}
	return true
}

func (p *ProducerWithOrder) waitForTasks(ctx context.Context, dependencies []commonmodels.TaskID) {
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

func (p *ProducerWithOrder) Task() (commoninterfaces.Restorer, error) {
	if p.err != nil {
		return nil, p.err
	}
	currentTaskID := p.meta.DumpStat.RestorationContext.RestorationOrder[p.lastIdx]

	restorationItem, ok := p.meta.DumpStat.RestorationItems[currentTaskID]
	if !ok {
		panic("no restoration item")
	}
	switch restorationItem.ObjectKind {
	case commonmodels.ObjectKindTable:
		opts := []restorers.Option{restorers.WithCompression(true, true)}
		return restorers.NewTableDataRestorer(restorationItem, p.connCfg, p.st, p.taskResolver, opts...)
	}
	return nil, fmt.Errorf("create restore task for kind '%s': %w",
		restorationItem.ObjectKind, errUnsupportedObjectKind)
}
