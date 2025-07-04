package utils

func New[T any](v T) *T {
	return &v
}

func Value[T any](v *T) T {
	if v == nil {
		var zero T
		return zero
	}
	return *v
}

func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
