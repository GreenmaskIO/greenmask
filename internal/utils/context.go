package utils

import "context"

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
