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

package validationcollector

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

var (
	ErrorCollectorNotFound = errors.New("validation collector not found in context")
)

// DefaultCollector is a global default collector. It is returned by FromContext if no collector is
// found in the context.
var DefaultCollector = NewCollector()

type contextKey struct{}

var collectorKey = contextKey{}

// WithCollector adds a Collector to the context, allowing it to be retrieved later.
func WithCollector(ctx context.Context, vc *Collector) context.Context {
	return context.WithValue(ctx, collectorKey, vc)
}

// WithMeta returns a context with a child Collector that adds `meta` to its context.
// All Add() calls on the child still append into the same root collector.
func WithMeta(ctx context.Context, pairs ...any) context.Context {
	return WithCollector(ctx, FromContext(ctx).WithMetaV2(pairs...))
}

// FromContext returns the Collector from the context, or nil if not found.
func FromContext(ctx context.Context) *Collector {
	if vc, ok := ctx.Value(collectorKey).(*Collector); ok {
		return vc
	}
	return DefaultCollector
}

// Collector gathers warnings, layering on context metadata.
// You can fork it with WithMeta, but all forks write back to the same root.
type Collector struct {
	// parent - set null on the root.
	parent *Collector
	// warning - stored only on the root.
	warnings []*models.ValidationWarning
	mu       sync.Mutex
	// Context of the current collector.
	contextMeta map[string]any
}

// NewCollector creates a brand-new (root) collector with empty meta.
func NewCollector() *Collector {
	return &Collector{
		contextMeta: make(map[string]any),
	}
}

func getMetaFromPairs(pairs ...any) map[string]any {
	if len(pairs)%2 != 0 {
		panic("pairs must have pairs")
	}
	meta := make(map[string]any, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if !ok {
			panic(fmt.Sprintf("key should be a string, got %T", pairs[i]))
		}
		value := pairs[i+1]
		meta[key] = value
	}
	return meta
}

// NewCollectorWithMeta create a new collector with the meta provided.
func NewCollectorWithMeta(pairs ...any) *Collector {
	return &Collector{
		contextMeta: getMetaFromPairs(pairs...),
	}
}

// WithMetaV2 returns a child collector that adds `meta` to its context.
// All Add() calls on the child still append into the same root collector.
func (vc *Collector) WithMetaV2(pairs ...any) *Collector {
	return vc.WithMeta(getMetaFromPairs(pairs...))
}

// WithMeta returns a child collector that adds `meta` to its context.
// All Add() calls on the child still append into the same root collector.
func (vc *Collector) WithMeta(meta map[string]any) *Collector {
	// merge parent contextMeta + new meta
	merged := make(map[string]any, len(vc.contextMeta)+len(meta))
	// Copy context meta from root.
	for k, v := range vc.Root().contextMeta {
		merged[k] = v
	}
	// Copy context meta current.
	for k, v := range vc.contextMeta {
		merged[k] = v
	}
	// Enrich a new Collector with provided meta.
	if meta != nil {
		for k, v := range meta {
			merged[k] = v
		}
	}

	return &Collector{
		parent:      vc.Root(),
		contextMeta: merged,
	}
}

// Add enriches vw with this collector’s metadata, then appends it to the root.
func (vc *Collector) Add(warnings ...*models.ValidationWarning) {
	for i := range warnings {
		vc.add(warnings[i])
	}
}

func (vc *Collector) add(vw *models.ValidationWarning) {
	if vc.parent != nil {
		vc.parent.Add(vw)
	}
	// apply all context metadata
	for k, v := range vc.contextMeta {
		vw.AddMeta(k, v)
	}
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.warnings = append(vc.warnings, vw)
}

// GetWarnings returns the slice of all warnings collected in the root.
func (vc *Collector) GetWarnings() []*models.ValidationWarning {
	vc.Root().mu.Lock()
	defer vc.Root().mu.Unlock()
	return vc.Root().warnings
}

func (vc *Collector) HasWarnings() bool {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return len(vc.warnings) > 0
}

// IsFatal returns true if any collected warning is fatal.
func (vc *Collector) IsFatal() bool {
	return slices.ContainsFunc(vc.warnings, func(warning *models.ValidationWarning) bool {
		return warning.Severity == models.ValidationSeverityError
	})
}

// Root walks up to the root collector.
func (vc *Collector) Root() *Collector {
	if vc.parent == nil {
		return vc
	}
	return vc.parent.Root()
}

func (vc *Collector) Len() int {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return len(vc.warnings)
}

func (vc *Collector) GetMeta() map[string]any {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return maps.Clone(vc.contextMeta)
}

func (vc *Collector) GetMetaKey(key string) (any, bool) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	v, ok := vc.contextMeta[key]
	return v, ok
}
