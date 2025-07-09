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
	"fmt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/rs/zerolog/log"

	commoninterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/tableruntime"
)

var endOfLineSeq = []byte("\n")

type transformationFunc func(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error)

type TransformationPipeline struct {
	tableRuntime *tableruntime.TableRuntime
	line         uint64
	row          commoninterfaces.RowDriver
}

func NewTransformationPipeline(tableRuntime *tableruntime.TableRuntime) *TransformationPipeline {
	return &TransformationPipeline{
		tableRuntime: tableRuntime,
	}
}

func (tp *TransformationPipeline) Init(ctx context.Context) error {
	var lastInitErr error
	var idx int
	var t *utils.TransformerContext
	for idx, t = range tp.tableRuntime.TransformerRuntimes {
		if err := t.Transformer.Init(ctx); err != nil {
			lastInitErr = err
			log.Warn().Err(err).Msg("error initializing transformer")
		}
	}

	if lastInitErr != nil {
		lastInitialized := idx
		for _, t = range tp.tableRuntime.TransformerRuntimes[:lastInitialized] {
			if err := t.Transformer.Done(ctx); err != nil {
				log.Warn().Err(err).Msg("error terminating previously initialized transformer")
			}
		}
	}
	if lastInitErr != nil {
		return fmt.Errorf("unable to initialize transformer: %w", lastInitErr)
	}

	return nil
}

func (tp *TransformationPipeline) Transform(ctx context.Context, r commoninterfaces.Recorder) error {
	for _, t := range tp.tableRuntime.TransformerRuntimes {
		needTransform, err := t.WhenCond.Evaluate(r)
		if err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error evaluating transformer")
			return fmt.Errorf("evaluate transformer condition: %w", err)
		}
		if !needTransform {
			continue
		}
		err = t.Transformer.Transform(ctx, r)
		if err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("transformation failed")
			return fmt.Errorf("transform record: %w", err)
		}
	}
	return nil
}

func (tp *TransformationPipeline) Done(ctx context.Context) error {
	var lastErr error
	for _, t := range tp.tableRuntime.TransformerRuntimes {
		if err := t.Transformer.Done(ctx); err != nil {
			lastErr = err
			log.Warn().Err(err).Msg("error terminating initialized transformer")
		}
	}

	if lastErr != nil {
		return fmt.Errorf("error terminating initialized transformer: %w", lastErr)
	}
	return nil
}
