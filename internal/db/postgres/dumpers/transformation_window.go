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
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type asyncContext struct {
	tc *utils.TransformerContext
	ch chan struct{}
}

type transformationWindow struct {
	affectedColumns map[string]struct{}
	window          []*asyncContext
	done            chan struct{}
	wg              *sync.WaitGroup
	eg              *errgroup.Group
	r               *toolkit.Record
	ctx             context.Context
}

func newTransformationWindow(ctx context.Context, eg *errgroup.Group) *transformationWindow {
	return &transformationWindow{
		affectedColumns: map[string]struct{}{},
		done:            make(chan struct{}, 1),
		wg:              &sync.WaitGroup{},
		eg:              eg,
		ctx:             ctx,
	}
}

func (tw *transformationWindow) tryAdd(table *entries.Table, t *utils.TransformerContext) bool {

	affectedColumn := t.Transformer.GetAffectedColumns()
	if len(affectedColumn) == 0 {
		if len(tw.window) == 0 {
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
	}

	tw.window = append(tw.window, &asyncContext{
		tc: t,
		ch: make(chan struct{}, 1),
	})

	return true
}

// init - runs all transformers in the goroutines and waits for the ac.ch signal to run the transformer
func (tw *transformationWindow) init() {
	for _, ac := range tw.window {
		func(ac *asyncContext) {
			tw.eg.Go(func() error {
				for {
					select {
					case <-tw.ctx.Done():
						return tw.ctx.Err()
					case <-tw.done:
						return nil
					case <-ac.ch:
					}
					_, err := ac.tc.Transformer.Transform(tw.ctx, tw.r)
					if err != nil {
						tw.wg.Done()
						return err
					}
					tw.wg.Done()
				}
			})
		}(ac)
	}
}

// close - closes the done channel to stop the transformers goroutines
func (tw *transformationWindow) close() {
	close(tw.done)
}

// Transform - runs the transformation for the record in the window. This function checks when
// condition of the transformer and if true sends a signal to the transformer goroutine to run the transformation
func (tw *transformationWindow) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	tw.r = r
	for _, ac := range tw.window {
		needTransform, err := ac.tc.EvaluateWhen(r)
		if err != nil {
			return nil, fmt.Errorf("error evaluating when condition: %w", err)
		}
		if !needTransform {
			continue
		}

		tw.wg.Add(1)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tw.ctx.Done():
			return nil, tw.ctx.Err()
		case ac.ch <- struct{}{}:

		}
	}

	tw.wg.Wait()
	return r, nil
}
