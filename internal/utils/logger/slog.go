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

package logger

import (
	"context"
	"log/slog"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// sourceField marks a record as relayed from a library.
const sourceField = "source"

// NewSlog returns a slog.Logger that writes to zerolog's global logger, read at every call
// so that a later logger change is honoured. Each record carries source, e.g. "storages".
func NewSlog(source string) *slog.Logger {
	return slog.New(&zerologHandler{source: source})
}

type zerologHandler struct {
	source string
	attrs  []groupedAttr
	prefix string
}

// groupedAttr keeps the group prefix apart from the key, so an inline group (empty key)
// expands under the prefix instead of into "prefix..key".
type groupedAttr struct {
	prefix string
	attr   slog.Attr
}

func zerologLevel(l slog.Level) zerolog.Level {
	switch {
	case l < slog.LevelInfo:
		return zerolog.DebugLevel
	case l < slog.LevelWarn:
		return zerolog.InfoLevel
	case l < slog.LevelError:
		return zerolog.WarnLevel
	default:
		return zerolog.ErrorLevel
	}
}

func (h *zerologHandler) Enabled(_ context.Context, l slog.Level) bool {
	lvl := zerologLevel(l)
	return lvl >= zerolog.GlobalLevel() && lvl >= log.Logger.GetLevel()
}

func (h *zerologHandler) Handle(_ context.Context, r slog.Record) error {
	e := log.Logger.WithLevel(zerologLevel(r.Level))
	if e == nil {
		return nil
	}
	if h.source != "" {
		e.Str(sourceField, h.source)
	}
	for _, a := range h.attrs {
		addAttr(e, a.prefix, a.attr)
	}
	r.Attrs(func(a slog.Attr) bool {
		addAttr(e, h.prefix, a)
		return true
	})
	e.Msg(r.Message)
	return nil
}

func (h *zerologHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	nh := &zerologHandler{source: h.source, prefix: h.prefix, attrs: make([]groupedAttr, 0, len(h.attrs)+len(attrs))}
	nh.attrs = append(nh.attrs, h.attrs...)
	for _, a := range attrs {
		nh.attrs = append(nh.attrs, groupedAttr{prefix: h.prefix, attr: a})
	}
	return nh
}

func (h *zerologHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &zerologHandler{source: h.source, attrs: h.attrs, prefix: h.prefix + name + "."}
}

func addAttr(e *zerolog.Event, prefix string, a slog.Attr) {
	// slog's Handler contract: an Attr whose key and value are both zero is ignored.
	if a.Equal(slog.Attr{}) {
		return
	}
	v := a.Value.Resolve()
	key := prefix + a.Key
	switch v.Kind() {
	case slog.KindGroup:
		groupPrefix := prefix
		if a.Key != "" {
			groupPrefix = key + "."
		}
		for _, ga := range v.Group() {
			addAttr(e, groupPrefix, ga)
		}
	case slog.KindString:
		e.Str(key, v.String())
	case slog.KindInt64:
		e.Int64(key, v.Int64())
	case slog.KindUint64:
		e.Uint64(key, v.Uint64())
	case slog.KindFloat64:
		e.Float64(key, v.Float64())
	case slog.KindBool:
		e.Bool(key, v.Bool())
	case slog.KindDuration:
		e.Dur(key, v.Duration())
	case slog.KindTime:
		e.Time(key, v.Time())
	default:
		if err, ok := v.Any().(error); ok {
			e.AnErr(key, err)
			return
		}
		e.Interface(key, v.Any())
	}
}
