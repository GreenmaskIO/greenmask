package core

type CondEvaluator interface {
	Evaluate(r Recorder) (bool, error)
	// Expression returns the original (normalized) condition expression, or ""
	// when no condition is configured. It lets callers snapshot the condition
	// without re-compiling it.
	Expression() string
}
