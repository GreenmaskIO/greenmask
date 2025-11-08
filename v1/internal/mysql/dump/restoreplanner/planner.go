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

package restoreplanner

import (
	"fmt"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type KindSetting struct {
	Kind commonmodels.ObjectKind
	// CanBeMixed - indicates if the object kind can be
	// planned parallel with other kinds. If not all kinds of
	// this type must be restored before any other further kinds will be restored.
	CanBeMixed bool
}

var (
	mysqlKindSettings = []KindSetting{
		{
			Kind:       commonmodels.ObjectKindTable,
			CanBeMixed: true,
		},
	}
)

type Planner struct {
}

func New(
	kindPriority []commonmodels.ObjectKind,
	kindObjectOrder map[commonmodels.ObjectKind][]commonmodels.ObjectID,
) {

}

func (tp *TaskProducer) getDependsOn(tableID int) []commonmodels.TaskID {
	dependencies := tp.s.GetTableGraph().Graph[tableID]
	res := make([]commonmodels.TaskID, 0, len(dependencies))
	for _, dependency := range dependencies {
		dumpID, ok := tp.tableID2DumpID[dependency.To().TableID()]
		if !ok {
			panic("table ID not found in dump ID map")
		}
		res = append(res, dumpID)
	}
	return res
}

func (tp *TaskProducer) GetRestorationPlan() (commonmodels.RestorationContext, error) {
	hasTopologicalOrder := true
	order, err := tp.s.GetTopologicalOrder()
	if err != nil {
		if errors.Is(err, commonmodels.ErrTableGraphHasCycles) {
			hasTopologicalOrder = false
		} else {
			return commonmodels.RestorationContext{}, fmt.Errorf("get topological order: %w", err)
		}
	}
	res := make([]commonmodels.RestorationItem, len(order))
	for i, tableID := range order {
		dumpID, ok := tp.tableID2DumpID[tableID]
		if !ok {
			return commonmodels.RestorationContext{}, fmt.Errorf("table ID %d not found in dump ID map", tableID)
		}
		res[i] = commonmodels.RestorationItem{
			TaskID:    dumpID,
			DependsOn: tp.getDependsOn(tableID),
			Kind:      commonmodels.ObjectKindTable,
			KindID:    tableID,
		}
	}
	return commonmodels.RestorationContext{
		HasTopologicalOrder: hasTopologicalOrder,
		RestorationItems:    res,
	}, nil
}
