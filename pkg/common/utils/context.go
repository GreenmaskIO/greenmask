package utils

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
)

type saltKey struct{}

func WithSalt(ctx context.Context, salt []byte) context.Context {
	return context.WithValue(ctx, saltKey{}, salt)
}

func SaltFromCtx(ctx context.Context) []byte {
	salt, ok := ctx.Value(saltKey{}).([]byte)
	if !ok {
		return nil
	}
	return salt
}

// WithSaltFromEnv reads the hex-encoded salt from the GREENMASK_GLOBAL_SALT
// environment variable and injects it into the context. If the variable is
// unset the context is returned unchanged with a nil salt.
func WithSaltFromEnv(ctx context.Context) (context.Context, error) {
	var salt []byte
	if saltHex := os.Getenv("GREENMASK_GLOBAL_SALT"); saltHex != "" {
		salt = make([]byte, hex.DecodedLen(len(saltHex)))
		if _, err := hex.Decode(salt, []byte(saltHex)); err != nil {
			return nil, fmt.Errorf("decode GREENMASK_GLOBAL_SALT: %w", err)
		}
	}
	return WithSalt(ctx, salt), nil
}
