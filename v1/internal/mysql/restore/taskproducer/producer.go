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
	"fmt"

	commoninterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	mysqlconfig "github.com/greenmaskio/greenmask/v1/internal/mysql/config"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/restore/restorers"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

type dummyTaskMapper struct{}

func (*dummyTaskMapper) SetTaskCompleted(_ commonmodels.TaskID) {
	// no-op
}

func (*dummyTaskMapper) IsTaskCompleted(_ commonmodels.TaskID) bool {
	return true
}

type Producer struct {
	meta    commonmodels.Metadata
	st      storages.Storager
	connCfg mysqlconfig.ConnectionOpts
	err     error
	lastIdx int
	taskIDs []commonmodels.TaskID
}

func New(
	meta commonmodels.Metadata,
	st storages.Storager,
	connCfg mysqlconfig.ConnectionOpts,
) *Producer {
	taskIDs := make([]commonmodels.TaskID, 0, len(meta.DumpStat.RestorationItems))
	for taskID := range meta.DumpStat.RestorationItems {
		taskIDs = append(taskIDs, taskID)
	}
	return &Producer{
		meta:    meta,
		st:      st,
		connCfg: connCfg,
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

func (p *Producer) Task() (commoninterfaces.Restorer, error) {
	if p.err != nil {
		return nil, p.err
	}
	taskID := p.taskIDs[p.lastIdx]
	restorationItem, ok := p.meta.DumpStat.RestorationItems[taskID]
	if !ok {
		panic("no restoration item")
	}
	switch restorationItem.ObjectKind {
	case commonmodels.ObjectKindTable:
		return restorers.NewTableDataRestorer(restorationItem, p.connCfg, p.st, &dummyTaskMapper{})
	}
	return nil, fmt.Errorf("create restore task for kind '%s': %w",
		restorationItem.ObjectKind, errUnsupportedObjectKind)
}
