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

package cli

import (
	"context"
	"errors"
	"fmt"

	commondelete "github.com/greenmaskio/greenmask/pkg/common/delete"
	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
)

var ErrDeleteWrongOptions = errors.New("wrong delete options")

type DeleteMode string

const (
	DeleteModePruneFailed  DeleteMode = "prune-failed"
	DeleteModeBeforeDate   DeleteMode = "before-date"
	DeleteModeRetainRecent DeleteMode = "retain-recent"
	DeleteModeRetainFor    DeleteMode = "retain-for"
	DeleteModeDumpID       DeleteMode = "dump-id"
)

type DeleteOptions struct {
	PruneFailed  bool
	PruneUnsafe  bool
	DryRun       bool
	RetainRecent int
	BeforeDate   string
	RetainFor    string
	DumpID       string
}

func (o *DeleteOptions) GetMode() (DeleteMode, error) {
	switch {
	case o.RetainFor != "":
		return DeleteModeRetainFor, nil
	case o.RetainRecent != -1:
		return DeleteModeRetainRecent, nil
	case o.PruneFailed:
		return DeleteModePruneFailed, nil
	case o.BeforeDate != "":
		return DeleteModeBeforeDate, nil
	case o.DumpID != "":
		return DeleteModeDumpID, nil
	default:
		return "", ErrDeleteWrongOptions
	}
}

func (o *DeleteOptions) Validate() error {
	if o.PruneUnsafe && !o.PruneFailed {
		return fmt.Errorf("--prune-unsafe works only with --prune-failed")
	}
	return nil
}

// ForDelete supplies the parameters for the Delete operation.
func (g *Cli) ForDelete(opts DeleteOptions) *Cli {
	g.deleteOpts = &opts
	return g
}

func (g *Cli) Delete(ctx context.Context) error {
	if g.deleteOpts == nil {
		return fmt.Errorf("delete options not set: call ForDelete before Delete")
	}
	if err := g.initInfrastructure(); err != nil {
		return fmt.Errorf("setup infrastructure: %w", err)
	}
	ctx = SetupContext(ctx, g.cfg)
	st, err := g.storage(ctx)
	if err != nil {
		return err
	}
	return g.runDeleteWithStorage(ctx, st)
}

func (g *Cli) runDeleteWithStorage(ctx context.Context, st interfaces.Storager) error {
	if err := g.deleteOpts.Validate(); err != nil {
		return err
	}
	mode, err := g.deleteOpts.GetMode()
	if err != nil {
		return fmt.Errorf("get delete mode: %w", err)
	}
	deleter := commondelete.New(st, g.cfg.Common.HeartbeatInterval)
	switch mode {
	case DeleteModeRetainFor:
		if err := deleter.RetainFor(ctx, g.deleteOpts.RetainFor, g.deleteOpts.DryRun); err != nil {
			return fmt.Errorf("retain for dumps: %w", err)
		}
	case DeleteModeRetainRecent:
		if err := deleter.RetainRecent(ctx, g.deleteOpts.RetainRecent, g.deleteOpts.DryRun); err != nil {
			return fmt.Errorf("retain recent dumps: %w", err)
		}
	case DeleteModePruneFailed:
		if err := deleter.PruneFailed(ctx, g.deleteOpts.PruneUnsafe, g.deleteOpts.DryRun); err != nil {
			return fmt.Errorf("prune failed dumps: %w", err)
		}
	case DeleteModeBeforeDate:
		if err := deleter.BeforeDate(ctx, g.deleteOpts.BeforeDate, g.deleteOpts.DryRun); err != nil {
			return fmt.Errorf("deleting dumps older than date: %w", err)
		}
	case DeleteModeDumpID:
		if err := deleter.ByDumpID(ctx, g.deleteOpts.DumpID, g.deleteOpts.DryRun); err != nil {
			return fmt.Errorf("delete dump by id: %w", err)
		}
	default:
		return fmt.Errorf("unknown delete mode %q", mode)
	}
	return nil
}
