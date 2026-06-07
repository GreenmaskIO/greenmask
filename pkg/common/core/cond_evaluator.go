package core

type CondEvaluator interface {
	Evaluate(r Recorder) (bool, error)
}
