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

package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"

	dumpcontext "github.com/greenmaskio/greenmask/v1/internal/common/dump/context"
	commoninterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
)

type TransformationPipeline struct {
	tableContext *dumpcontext.TableContext
	line         uint64
	row          commoninterfaces.RowDriver
}

func NewTransformationPipeline(tableContext *dumpcontext.TableContext) *TransformationPipeline {
	return &TransformationPipeline{
		tableContext: tableContext,
	}
}

func (tp *TransformationPipeline) Init(ctx context.Context) error {
	var lastInitErr error
	var i int
	var t *dumpcontext.TransformerContext
	for i, t = range tp.tableContext.TransformerContext {
		if err := t.Transformer.Init(ctx); err != nil {
			lastInitErr = errors.Join(
				lastInitErr,
				fmt.Errorf("initialize transformer '%s'[%d]", t.Transformer.Describe(), i),
			)
			log.Ctx(ctx).
				Warn().
				Int("Position", i).
				Str("TransformerName", t.Transformer.Describe()).
				Err(err).
				Msg("error initializing transformer")
		}
	}

	if lastInitErr != nil {
		lastInitialized := i
		for i, t := range tp.tableContext.TransformerContext[:lastInitialized] {
			if err := t.Transformer.Done(ctx); err != nil {
				log.Ctx(ctx).
					Warn().
					Int("Position", i+lastInitialized).
					Str("TransformerName", t.Transformer.Describe()).
					Err(err).
					Msg("error terminating transformer")
			}
		}
	}
	if lastInitErr != nil {
		return fmt.Errorf("initialize transformer: %w", lastInitErr)
	}

	return nil
}

func (tp *TransformationPipeline) Transform(ctx context.Context, r commoninterfaces.Recorder) error {
	needTransform, err := tp.tableContext.EvaluateWhen(r)
	if err != nil {
		return fmt.Errorf("evaluate table condition: %w", err)
	}
	if !needTransform {
		return nil
	}
	for i, t := range tp.tableContext.TransformerContext {
		needTransform, err := t.EvaluateWhen(r)
		if err != nil {
			tranName := t.Transformer.Describe()
			return fmt.Errorf("evaluate transformer '%s'[%d] condition: %w", tranName, i, err)
		}
		if !needTransform {
			continue
		}
		err = t.Transformer.Transform(ctx, r)
		if err != nil {
			tranName := t.Transformer.Describe()
			return fmt.Errorf("transform record using '%s'[%d] transformer : %w", tranName, i, err)
		}
	}
	return nil
}

func (tp *TransformationPipeline) Done(ctx context.Context) error {
	var lastErr error
	for i, t := range tp.tableContext.TransformerContext {
		if err := t.Transformer.Done(ctx); err != nil {
			lastErr = errors.Join(
				lastErr,
				fmt.Errorf("initialize transformer '%s'[%d]", t.Transformer.Describe(), i),
			)
			log.Ctx(ctx).
				Warn().
				Int("Position", i).
				Str("TransformerName", t.Transformer.Describe()).
				Err(err).
				Msg("error initializing transformer")
		}
	}

	if lastErr != nil {
		return fmt.Errorf("terminate transformer: %w", lastErr)
	}
	return nil
}
