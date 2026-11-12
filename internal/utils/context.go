package utils

import (
	"context"
	"time"
)

// RuntimeOptions - global runtime settings passed through the context.
type RuntimeOptions struct {
	Salt     []byte
	Timezone *time.Location
}

type runtimeOptionsKey struct{}

// WithRuntimeOptions stores a copy of o in the context.
func WithRuntimeOptions(ctx context.Context, o RuntimeOptions) context.Context {
	return context.WithValue(ctx, runtimeOptionsKey{}, o)
}

// RuntimeOptionsFromCtx returns the stored options or the zero value.
func RuntimeOptionsFromCtx(ctx context.Context) RuntimeOptions {
	o, ok := ctx.Value(runtimeOptionsKey{}).(RuntimeOptions)
	if !ok {
		return RuntimeOptions{}
	}
	return o
}

func WithSalt(ctx context.Context, salt []byte) context.Context {
	o := RuntimeOptionsFromCtx(ctx)
	o.Salt = salt
	return WithRuntimeOptions(ctx, o)
}

func SaltFromCtx(ctx context.Context) []byte {
	return RuntimeOptionsFromCtx(ctx).Salt
}

// TimezoneFromCtx returns the timestamptz decoding location, UTC if not set.
func TimezoneFromCtx(ctx context.Context) *time.Location {
	if loc := RuntimeOptionsFromCtx(ctx).Timezone; loc != nil {
		return loc
	}
	return time.UTC
}
