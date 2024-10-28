package utils

import (
	"context"
	"testing"
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
