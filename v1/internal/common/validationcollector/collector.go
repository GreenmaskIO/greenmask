package validationcollector

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

var (
	ErrorCollectorNotFound = errors.New("validation collector not found in context")
)

type contextKey struct{}

var collectorKey = contextKey{}

// WithCollector adds a Collector to the context, allowing it to be retrieved later.
func WithCollector(ctx context.Context, vc *Collector) context.Context {
	return context.WithValue(ctx, collectorKey, vc)
}

// FromContext returns the Collector from the context, or nil if not found.
func FromContext(ctx context.Context) (*Collector, error) {
	if vc, ok := ctx.Value(collectorKey).(*Collector); ok {
		return vc, nil
	}
	return nil, ErrorCollectorNotFound
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

// NewCollector creates a brand-new (root) collector.
func NewCollector() *Collector {
	return &Collector{
		contextMeta: make(map[string]any),
	}
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
