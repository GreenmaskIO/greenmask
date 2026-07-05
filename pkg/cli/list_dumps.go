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
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"

	"github.com/greenmaskio/greenmask/pkg/common/commands/listdump"
	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/config"
)

// MetadataJsonFileName is kept for backward compatibility with validate.go
// and any external callers.
const MetadataJsonFileName = listdump.MetadataFileName

// Type aliases so that callers in cmd/ and cli.go require no changes.
type Filter = listdump.Filter
type DumpListItem = listdump.DumpListItem

// NewFilter delegates to listdump.NewFilter, keeping the cli API stable.
func NewFilter(tags []string, statuses []string) (*Filter, error) {
	return listdump.NewFilter(tags, statuses)
}

// ForListDumps supplies the parameters for the ListDumps operation.
func (g *Cli) ForListDumps(quiet bool, format OutputFormat, f *Filter) *Cli {
	g.listDumpsQuiet = quiet
	g.listDumpsFormat = format
	g.listDumpsFilter = f
	return g
}

func (g *Cli) ListDumps(ctx context.Context) error {
	if err := g.initInfrastructure(); err != nil {
		return fmt.Errorf("setup infrastructure: %w", err)
	}
	ctx = SetupContext(ctx, g.cfg)
	st, err := g.storage(ctx)
	if err != nil {
		return err
	}
	f := g.listDumpsFilter
	if f == nil {
		f = &Filter{}
	}
	return listDumpsWithStorage(ctx, g.cfg, st, g.listDumpsQuiet, g.listDumpsFormat, f)
}

// RunListDumps is the CLI entry point for the list-dumps command.
func RunListDumps(cfg *config.Config, quiet bool, format OutputFormat, f *Filter) error {
	ctx := context.Background()
	ctx = SetupContext(ctx, cfg)
	st, err := utils.GetStorage(ctx, cfg)
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}
	return listDumpsWithStorage(ctx, cfg, st, quiet, format, f)
}

func listDumpsWithStorage(ctx context.Context, cfg *config.Config, st core.Storager, quiet bool, format OutputFormat, f *Filter) error {
	if f == nil {
		f = &Filter{}
	}
	lister := listdump.New(st, cfg.Common.HeartbeatInterval)
	if quiet {
		ids, err := lister.IDs(ctx, f)
		if err != nil {
			return fmt.Errorf("list dump IDs: %w", err)
		}
		return printDumpIDs(ids)
	}
	items, err := lister.List(ctx, f)
	if err != nil {
		return fmt.Errorf("list dumps: %w", err)
	}
	if format == OutputFormatJSON {
		return printDumpJSON(items)
	}
	return printDumpTablePretty(items)
}

func printDumpIDs(ids []string) error {
	for _, id := range ids {
		fmt.Println(id)
	}
	return nil
}

func printDumpTablePretty(items []DumpListItem) error {
	data := make([][]string, 0, len(items))
	for _, info := range items {
		description := info.Description
		if len(description) > 60 {
			description = description[:57] + "..."
		}
		data = append(data, []string{
			info.ID,
			info.Date,
			info.Engine,
			strings.Join(info.Databases, ", "),
			utils.SizePretty(info.Size),
			utils.SizePretty(info.CompressedSize),
			info.Duration,
			fmt.Sprintf("%t", info.Transformed),
			string(info.Status),
			description,
			strings.Join(info.Tags, ", "),
		})
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{
		"id", "date", "engine",
		"databases", "size", "compressed size",
		"duration", "transformed", "status",
		"description", "tags",
	})
	table.AppendBulk(data)
	table.Render()
	return nil
}

func printDumpJSON(items []DumpListItem) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(items)
}
