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

package dumpers

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"

	dump "github.com/greenmaskio/greenmask/internal/db/postgres/dump_objects"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type TransformationWindow struct {
	affectedColumns map[string]struct{}
	transformers    []utils.Transformer
	chs             []chan struct{}
	done            chan struct{}
	wg              *sync.WaitGroup
	eg              *errgroup.Group
	r               *toolkit.Record
	ctx             context.Context
	size            int
}

func NewTransformationWindow(ctx context.Context, eg *errgroup.Group) *TransformationWindow {
	return &TransformationWindow{
		affectedColumns: map[string]struct{}{},
		done:            make(chan struct{}, 1),
		wg:              &sync.WaitGroup{},
		eg:              eg,
		ctx:             ctx,
	}
}

func (tw *TransformationWindow) TryAdd(table *dump.Table, t utils.Transformer) bool {

	affectedColumn := t.GetAffectedColumns()
	if len(affectedColumn) == 0 {
		if len(tw.transformers) == 0 {
			tw.transformers = append(tw.transformers, t)
			for _, c := range table.Columns {
				tw.affectedColumns[c.Name] = struct{}{}
			}
		} else {
			return false
		}
	} else {
		for _, name := range affectedColumn {
			if _, ok := tw.affectedColumns[name]; ok {
				return false
			}
		}
		for _, name := range affectedColumn {
			tw.affectedColumns[name] = struct{}{}
		}
		tw.transformers = append(tw.transformers, t)
	}

	ch := make(chan struct{}, 1)
	tw.chs = append(tw.chs, ch)
	tw.size++

	return true
}

func (tw *TransformationWindow) Init() {
	for idx, t := range tw.transformers {
		ch := tw.chs[idx]
		func(t utils.Transformer, ch chan struct{}) {
			tw.eg.Go(func() error {
				for {
					select {
					case <-tw.ctx.Done():
						return tw.ctx.Err()
					case <-tw.done:
						return nil
					case <-ch:
					}
					_, err := t.Transform(tw.ctx, tw.r)
					if err != nil {
						tw.wg.Done()
						return err
					}
					tw.wg.Done()
				}
			})
		}(t, ch)
	}
}

func (tw *TransformationWindow) Done() {
	close(tw.done)
}

func (tw *TransformationWindow) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	tw.wg.Add(tw.size)
	tw.r = r
	for _, ch := range tw.chs {

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tw.ctx.Done():
			return nil, tw.ctx.Err()
		case ch <- struct{}{}:

		}
	}

	tw.wg.Wait()
	return r, nil
}
