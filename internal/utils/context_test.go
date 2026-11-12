package utils

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextSalt(t *testing.T) {
	ctx := context.Background()
	salt := []byte("some_salt")
	ctx = WithSalt(ctx, salt)
	got := SaltFromCtx(ctx)
	if string(got) != string(salt) {
		t.Errorf("expected %s, got %s", salt, got)
	}
}

func TestRuntimeOptions_FromCtx(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	t.Run("empty context", func(t *testing.T) {
		ctx := context.Background()
		assert.Nil(t, SaltFromCtx(ctx))
		assert.Equal(t, time.UTC, TimezoneFromCtx(ctx))
		assert.Equal(t, RuntimeOptions{}, RuntimeOptionsFromCtx(ctx))
	})

	t.Run("options are readable", func(t *testing.T) {
		ctx := WithRuntimeOptions(context.Background(), RuntimeOptions{Salt: []byte("salt"), Timezone: berlin})
		assert.Equal(t, []byte("salt"), SaltFromCtx(ctx))
		assert.Equal(t, berlin, TimezoneFromCtx(ctx))
	})

	t.Run("WithSalt keeps the timezone and the parent options", func(t *testing.T) {
		parent := WithRuntimeOptions(context.Background(), RuntimeOptions{Salt: []byte("old"), Timezone: berlin})
		child := WithSalt(parent, []byte("new"))
		assert.Equal(t, []byte("new"), SaltFromCtx(child))
		assert.Equal(t, berlin, TimezoneFromCtx(child))
		assert.Equal(t, []byte("old"), SaltFromCtx(parent))
	})
}
